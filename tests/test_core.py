"""核心语义测试：python3 -m unittest discover -s tests -v"""
from __future__ import annotations

import json
import threading
import unittest

from crd_migrator import crd
from crd_migrator.app import SimulationApp
from crd_migrator.conversion import STASH_ANNOTATION, Converter, ConversionError, get_path
from crd_migrator.seed import seed_objects
from crd_migrator.simulator import TypedClient
from crd_migrator.store import ConflictError


def make_app() -> SimulationApp:
    return SimulationApp()


class ConversionTests(unittest.TestCase):
    def setUp(self):
        self.cv = Converter()
        self.obj = seed_objects()[0]

    def test_structural_mapping_both_directions(self):
        v1 = self.cv.convert_structural(self.obj, crd.V1)
        self.assertEqual(get_path(v1, "spec.retryPolicy.maxRetries"), 5)
        self.assertNotIn("retries", v1["spec"])
        back = self.cv.convert_structural(v1, crd.V1ALPHA1)
        self.assertEqual(get_path(back, "spec.retries"), 5)

    def test_version_only_fields_dropped_in_structural_conversion(self):
        v1 = self.cv.convert_structural(self.obj, crd.V1)
        self.assertNotIn("legacyTag", v1["spec"])
        v1_obj = {
            "apiVersion": crd.CRDDefinition.api_version(crd.V1),
            "kind": crd.KIND,
            "metadata": {"name": "x"},
            "spec": {"timeoutSeconds": 9, "retryPolicy": {"maxRetries": 1, "backoffSeconds": 4}},
        }
        alpha = self.cv.convert_structural(v1_obj, crd.V1ALPHA1)
        self.assertNotIn("timeoutSeconds", alpha["spec"])

    def test_round_trip_check_pure_structural(self):
        # 不含独占字段的对象应无损往返
        plain = {
            "apiVersion": crd.CRDDefinition.api_version(crd.V1ALPHA1),
            "kind": crd.KIND,
            "metadata": {"name": "x"},
            "spec": {"image": "img", "replicas": 2, "retries": 3},
        }
        self.assertEqual(self.cv.round_trip_check(plain), [])

    def test_conversion_review_protocol(self):
        review = {
            "request": {
                "uid": "r1",
                "desiredAPIVersion": crd.CRDDefinition.api_version(crd.V1),
                "objects": [self.obj],
            }
        }
        resp = self.cv.serve_conversion_review(review)
        self.assertEqual(resp["response"]["result"]["status"], "Success")
        converted = resp["response"]["convertedObjects"][0]
        self.assertEqual(converted["apiVersion"], crd.CRDDefinition.api_version(crd.V1))

    def test_conversion_review_failure(self):
        review = {"request": {"uid": "r2", "desiredAPIVersion": "bad/v9", "objects": [self.obj]}}
        resp = self.cv.serve_conversion_review(review)
        self.assertEqual(resp["response"]["result"]["status"], "Failed")


class FieldProtectionTests(unittest.TestCase):
    def setUp(self):
        self.app = make_app()
        self.name = "edge-delivery-a"

    def test_old_client_does_not_erase_new_fields(self):
        # 1) 新客户端先写入 v1 独占字段
        new_client = TypedClient(self.app.store, crd.V1, "nextgen")
        new_client.patch(self.name, {"timeoutSeconds": 42, "nodeSelector": {"zone": "z1"}})

        # 2) 旧客户端（结构体里根本没有这些字段）做一次更新
        old_client = TypedClient(self.app.store, crd.V1ALPHA1, "legacy")
        old_client.patch(self.name, {"replicas": 9})

        # 3) 存储仍是旧版本格式，但 v1 字段活在 stash 里
        stored = self.app.store.get_stored(self.name)
        self.assertEqual(stored["apiVersion"], crd.CRDDefinition.api_version(crd.V1ALPHA1))
        stash = json.loads(stored["metadata"]["annotations"][STASH_ANNOTATION])
        self.assertEqual(stash["spec.timeoutSeconds"], 42)
        self.assertEqual(stash["spec.nodeSelector"], {"zone": "z1"})
        self.assertEqual(stored["spec"]["replicas"], 9)

        # 4) 新客户端重新读取，独占字段原样回来
        view = self.app.store.get_view(self.name, crd.V1)
        self.assertEqual(get_path(view, "spec.timeoutSeconds"), 42)
        self.assertEqual(get_path(view, "spec.nodeSelector.zone"), "z1")
        # 结构映射也正确
        self.assertEqual(get_path(view, "spec.retryPolicy.maxRetries"), 5)

    def test_new_client_does_not_erase_legacy_fields(self):
        new_client = TypedClient(self.app.store, crd.V1, "nextgen")
        new_client.patch(self.name, {"timeoutSeconds": 7})
        stored = self.app.store.get_stored(self.name)
        # 存储仍是 v1alpha1：legacyTag 是正式字段，timeoutSeconds 在 stash
        self.assertEqual(stored["spec"]["legacyTag"], "canary-2026-q4")
        stash = json.loads(stored["metadata"]["annotations"][STASH_ANNOTATION])
        self.assertEqual(stash["spec.timeoutSeconds"], 7)
        # 旧版本视图里两个版本的字段都应可见
        view = self.app.store.get_view(self.name, crd.V1ALPHA1)
        self.assertEqual(view["spec"]["legacyTag"], "canary-2026-q4")
        v1view = self.app.store.get_view(self.name, crd.V1)
        self.assertEqual(get_path(v1view, "spec.timeoutSeconds"), 7)

        # 翻转存储后，legacyTag 转为 stash 中的兜底副本，仍不丢失
        self.app.migrator.update_config({"stepDelay": 0.01})
        self.app.migrator.flip_storage_version(crd.V1)
        import time
        for _ in range(100):
            if self.app.migrator.status()["phase"] == "succeeded":
                break
            time.sleep(0.05)
        migrated = self.app.store.get_stored(self.name)
        stash2 = json.loads(migrated["metadata"]["annotations"][STASH_ANNOTATION])
        self.assertEqual(stash2["spec.legacyTag"], "canary-2026-q4")
        back_view = self.app.store.get_view(self.name, crd.V1ALPHA1)
        self.assertEqual(back_view["spec"]["legacyTag"], "canary-2026-q4")

    def test_explicit_delete_of_only_field(self):
        new_client = TypedClient(self.app.store, crd.V1, "nextgen")
        new_client.patch(self.name, {"timeoutSeconds": 42})
        # 显式置 None 表示删除
        def mutate(obj):
            obj["spec"]["timeoutSeconds"] = None
        new_client.update(self.name, mutate)
        stored = self.app.store.get_stored(self.name)
        raw = get_path(stored, f"metadata.annotations.{STASH_ANNOTATION}")
        stash = json.loads(raw) if raw else {}
        self.assertNotIn("spec.timeoutSeconds", stash)


class OptimisticConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.app = make_app()
        self.name = "edge-delivery-b"

    def test_stale_resource_version_raises_409(self):
        old = TypedClient(self.app.store, crd.V1ALPHA1, "legacy")
        view = old.get(self.name)
        stale_rv = view["metadata"]["resourceVersion"]
        old.patch(self.name, {"replicas": 4})  # 先写一次，rv 已变
        with self.assertRaises(ConflictError):
            self.app.store.update(view, expected_rv=stale_rv)

    def test_blast_both_clients_eventually_succeed_and_fields_survive(self):
        results = self.app.blast.run(
            self.name,
            old_patch={"replicas": 6},
            new_patch={"timeoutSeconds": 25},
        )
        self.assertTrue(all(r["ok"] for r in results), results)
        attempts = sum(r["attempts"] for r in results)
        self.assertGreaterEqual(attempts, 2)  # 至少两个读-改-写循环

        stored = self.app.store.get_stored(self.name)
        stash = json.loads(stored["metadata"]["annotations"][STASH_ANNOTATION])
        self.assertEqual(stash["spec.timeoutSeconds"], 25)
        self.assertEqual(stored["spec"]["replicas"], 6)

    def test_parallel_updates_no_lost_update(self):
        counter = {"n": 0}
        barrier = threading.Barrier(5)

        def worker():
            client = TypedClient(self.app.store, crd.V1ALPHA1, f"c{threading.get_ident()}")
            barrier.wait()
            client.update(self.name, lambda obj: counter.__setitem__("n", counter["n"] + 1)
                          or obj["spec"].__setitem__("replicas", counter["n"]))

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)
        stored = self.app.store.get_stored(self.name)
        self.assertEqual(stored["spec"]["replicas"], 5)  # 无丢失更新


class MigratorTests(unittest.TestCase):
    def setUp(self):
        self.app = make_app()

    def _wait_phase(self, expected, timeout=10):
        import time
        deadline = time.time() + timeout
        while time.time() < deadline:
            phase = self.app.migrator.status()["phase"]
            if phase == expected:
                return
            time.sleep(0.05)
        self.fail(f"阶段未在 {timeout}s 内到达 {expected}，当前 {self.app.migrator.status()['phase']}")

    def test_full_migration_rewrites_storage_and_keeps_fields(self):
        self.app.migrator.update_config({"stepDelay": 0.01})
        self.app.migrator.flip_storage_version(crd.V1)
        self._wait_phase("succeeded")

        for record in self.app.store.list():
            self.assertEqual(record["migrationState"], "migrated")
            self.assertEqual(record["stored"]["apiVersion"],
                             crd.CRDDefinition.api_version(crd.V1))
        # 旧独占字段仍可通过旧版本视图读到
        view = self.app.store.get_view("edge-delivery-a", crd.V1ALPHA1)
        self.assertEqual(view["spec"]["legacyTag"], "canary-2026-q4")
        self.assertEqual(view["spec"]["upstream"]["legacyEndpoint"],
                         "http://legacy-svc:8088")
        v1view = self.app.store.get_view("edge-delivery-a", crd.V1)
        self.assertEqual(get_path(v1view, "spec.retryPolicy.maxRetries"), 5)

    def test_failure_injection_then_retry(self):
        self.app.migrator.update_config(
            {"stepDelay": 0.01, "maxRetries": 0, "failOnNames": ("edge-delivery-c",)}
        )
        self.app.migrator.flip_storage_version(crd.V1)
        self._wait_phase("failed")
        self.assertEqual(
            self.app.store.views("edge-delivery-c")["migrationState"], "failed"
        )
        # 清除故障后重试
        self.app.migrator.update_config({"failOnNames": ()})
        self.app.migrator.retry_failed()
        self._wait_phase("succeeded")

    def test_transient_failure_recovers_within_retry_budget(self):
        # 注入故障但给 2 次重试预算：首次尝试失败（瞬时 webhook 抖动），
        # 后续尝试不再触发注入，对象应在同一轮自动迁移成功
        self.app.migrator.update_config(
            {"stepDelay": 0.01, "conflictBackoff": 0.01,
             "maxRetries": 2, "failOnNames": ("edge-delivery-b",)}
        )
        self.app.migrator.flip_storage_version(crd.V1)
        self._wait_phase("succeeded")
        record = self.app.store.views("edge-delivery-b")
        self.assertEqual(record["migrationState"], "migrated")
        # 历史里该对象有一次失败后成功的记录
        entries = [h for h in self.app.migrator.history if h["name"] == "edge-delivery-b"]
        self.assertTrue(entries)
        self.assertTrue(any(h["ok"] and h["attempts"] >= 2 for h in entries))

    def test_rollback_restores_old_storage_and_controller(self):
        self.app.migrator.update_config(
            {"stepDelay": 0.01, "maxRetries": 0, "failOnNames": ("edge-delivery-c",)}
        )
        self.app.migrator.flip_storage_version(crd.V1)
        self._wait_phase("failed")

        self.app.migrator.rollback()
        self._wait_phase("rolled_back")
        self.assertEqual(self.app.definition.storage_version, crd.V1ALPHA1)
        for record in self.app.store.list():
            self.assertEqual(record["stored"]["apiVersion"],
                             crd.CRDDefinition.api_version(crd.V1ALPHA1))
        # 回滚后旧控制器可以正常读写
        old = TypedClient(self.app.store, crd.V1ALPHA1, "legacy")
        old.patch("edge-delivery-a", {"replicas": 11})
        self.assertEqual(self.app.store.get_stored("edge-delivery-a")["spec"]["replicas"], 11)

    def test_finalize_blocked_then_allowed(self):
        self.app.migrator.update_config(
            {"stepDelay": 0.01, "maxRetries": 0, "failOnNames": ("edge-delivery-b",)}
        )
        self.app.migrator.flip_storage_version(crd.V1)
        self._wait_phase("failed")
        with self.assertRaises(RuntimeError):
            self.app.migrator.finalize()

        self.app.migrator.update_config({"failOnNames": ()})
        self.app.migrator.retry_failed()
        self._wait_phase("succeeded")
        status = self.app.migrator.finalize()
        self.assertEqual(status["phase"], "finalized")
        self.assertNotIn(crd.V1ALPHA1, self.app.definition.served_versions)
        # finalize 后不能回滚
        with self.assertRaises(RuntimeError):
            self.app.migrator.rollback()
        # 旧版本请求被拒
        with self.assertRaises(Exception):
            self.app.store.get_view("edge-delivery-a", crd.V1ALPHA1)

    def test_migration_conflict_with_concurrent_client(self):
        # 慢速迁移，给并发写入留窗口
        self.app.migrator.update_config({"stepDelay": 0.15, "maxRetries": 5,
                                         "conflictBackoff": 0.05})
        self.app.migrator.flip_storage_version(crd.V1)

        # 迁移进行中持续从两个版本写入
        def writer(version, patch):
            client = TypedClient(self.app.store, crd.V1 if version == "v1" else crd.V1ALPHA1,
                                 f"writer-{version}")
            for _ in range(4):
                try:
                    client.patch("edge-delivery-a", patch)
                except Exception:
                    pass
                import time
                time.sleep(0.08)

        t1 = threading.Thread(target=writer, args=("v1alpha1", {"replicas": 3}))
        t2 = threading.Thread(target=writer, args=("v1", {"timeoutSeconds": 99}))
        t1.start(); t2.start()
        t1.join(); t2.join()

        self._wait_phase("succeeded", timeout=15)
        stored = self.app.store.get_stored("edge-delivery-a")
        # v1 存储下 timeoutSeconds 已是正式字段；legacyTag 由 stash 兜底
        self.assertEqual(stored["spec"]["timeoutSeconds"], 99)
        stash = json.loads(stored["metadata"]["annotations"][STASH_ANNOTATION])
        self.assertEqual(stash["spec.legacyTag"], "canary-2026-q4")
        # 旧版本视图仍能读回全部旧字段
        alpha_view = self.app.store.get_view("edge-delivery-a", crd.V1ALPHA1)
        self.assertEqual(alpha_view["spec"]["legacyTag"], "canary-2026-q4")


if __name__ == "__main__":
    unittest.main()
