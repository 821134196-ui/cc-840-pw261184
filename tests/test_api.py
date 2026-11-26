"""HTTP 层冒烟测试：真实起一个线程内服务器，走完整 API。"""
from __future__ import annotations

import json
import threading
import time
import unittest
import urllib.error
import urllib.request

from crd_migrator.apiserver import build_server


def http(method: str, url: str, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


class ApiSmokeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server, cls.app = build_server("127.0.0.1", 0)
        cls.port = cls.server.server_address[1]
        cls.base = f"http://127.0.0.1:{cls.port}"
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.app.migrator.shutdown()
        cls.server.shutdown()

    def test_01_overview_and_static(self):
        status, data = http("GET", f"{self.base}/api/overview")
        self.assertEqual(status, 200)
        self.assertEqual(len(data["objects"]), 3)
        with urllib.request.urlopen(f"{self.base}/", timeout=5) as resp:
            self.assertIn(b"CRD", resp.read())

    def test_02_patch_old_client_keeps_v1_fields(self):
        http("POST", f"{self.base}/api/objects/edge-delivery-a/patch",
             {"version": "v1", "specPatch": {"timeoutSeconds": 77}})
        status, _ = http("POST", f"{self.base}/api/objects/edge-delivery-a/patch",
                         {"version": "v1alpha1", "specPatch": {"replicas": 2}})
        self.assertEqual(status, 200)
        _, views = http("GET", f"{self.base}/api/objects/edge-delivery-a")
        self.assertEqual(views["views"]["v1"]["spec"]["timeoutSeconds"], 77)

    def test_03_blast(self):
        status, data = http("POST", f"{self.base}/api/objects/edge-delivery-b/blast",
                            {"oldPatch": {"replicas": 4},
                             "newPatch": {"timeoutSeconds": 33}})
        self.assertEqual(status, 200)
        self.assertTrue(all(r["ok"] for r in data["results"]))

    def test_04_conflict_on_stale_rv(self):
        _, views = http("GET", f"{self.base}/api/objects/edge-delivery-b")
        stale = views["views"]["v1alpha1"]
        http("POST", f"{self.base}/api/objects/edge-delivery-b/patch",
             {"version": "v1alpha1", "specPatch": {"replicas": 1}})
        status, err = http("PUT", f"{self.base}/api/objects/edge-delivery-b", stale)
        self.assertEqual(status, 409)
        self.assertEqual(err["error"], "ConflictError")

    def test_05_migration_with_failure_rollback_and_finalize(self):
        http("POST", f"{self.base}/api/migrator/config",
             {"stepDelay": 0.01, "maxRetries": 0, "failOnNames": ["edge-delivery-c"]})
        status, mig = http("POST", f"{self.base}/api/migrate/flip", {"version": "v1"})
        self.assertEqual(status, 200)

        deadline = time.time() + 10
        while time.time() < deadline:
            _, mig = http("GET", f"{self.base}/api/migrator")
            if mig["phase"] == "failed":
                break
            time.sleep(0.05)
        self.assertEqual(mig["phase"], "failed")

        # finalize 必须被拦截
        status, err = http("POST", f"{self.base}/api/migrate/finalize", {})
        self.assertEqual(status, 409)

        # 回滚
        status, mig = http("POST", f"{self.base}/api/migrate/rollback", {})
        self.assertEqual(status, 200)
        deadline = time.time() + 10
        while time.time() < deadline:
            _, mig = http("GET", f"{self.base}/api/migrator")
            if mig["phase"] == "rolled_back":
                break
            time.sleep(0.05)
        self.assertEqual(mig["phase"], "rolled_back")

        # 清除故障，重新迁移到成功
        http("POST", f"{self.base}/api/migrator/config",
             {"stepDelay": 0.01, "failOnNames": []})
        http("POST", f"{self.base}/api/migrate/flip", {"version": "v1"})
        deadline = time.time() + 10
        while time.time() < deadline:
            _, mig = http("GET", f"{self.base}/api/migrator")
            if mig["phase"] == "succeeded":
                break
            time.sleep(0.05)
        self.assertEqual(mig["phase"], "succeeded")

        status, mig = http("POST", f"{self.base}/api/migrate/finalize", {})
        self.assertEqual(status, 200)
        self.assertEqual(mig["phase"], "finalized")

        # 旧版本下线后请求被拒
        status, err = http(
            "POST", f"{self.base}/api/objects/edge-delivery-a/patch",
            {"version": "v1alpha1", "specPatch": {"replicas": 1}},
        )
        self.assertEqual(status, 410)

    def test_06_conversion_review_endpoint(self):
        status, data = http("POST", f"{self.base}/api/conversion-review", {
            "request": {
                "uid": "u1",
                "desiredAPIVersion": "delivery.pointwise.io/v1",
                "objects": [{
                    "apiVersion": "delivery.pointwise.io/v1alpha1",
                    "kind": "PointwiseJob",
                    "metadata": {"name": "z"},
                    "spec": {"retries": 4},
                }],
            }
        })
        self.assertEqual(status, 200)
        self.assertEqual(
            data["response"]["convertedObjects"][0]["spec"]["retryPolicy"]["maxRetries"], 4
        )


if __name__ == "__main__":
    unittest.main()
