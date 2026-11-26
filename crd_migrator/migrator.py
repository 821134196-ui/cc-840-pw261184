"""存储版本迁移控制器（对标 kube-storage-version-migrator）。

真实流程：
1. 把 CRD 的 storage 版本从 v1alpha1 切到 v1（新对象开始以 v1 落盘）
2. 迁移器发现存储里仍有旧格式对象，逐个「读出 -> 转换 -> CAS 重写」
3. 全部完成后，才允许下线旧版本（finalize），届时回滚窗口关闭

本控制器额外支持：
- 失败注入（按对象名 / 概率），失败对象进入 failed，允许重试
- 回滚：在 finalize 之前把 storage 版本和所有对象写回旧格式
"""
from __future__ import annotations

import itertools
import threading
import time
from dataclasses import dataclass
from typing import Any

from . import crd
from .conversion import Converter, get_path
from .store import ConflictError, EtcdStore

_MISSING_MARK = object()

# 迁移阶段
IDLE = "idle"                # 还没翻转存储版本
IN_PROGRESS = "in_progress"  # 已翻转，后台迁移中
SUCCEEDED = "succeeded"      # 所有对象完成重写（仍可回滚）
FAILED = "failed"            # 有对象失败且达到重试上限（仍可回滚 / 重试）
ROLLING_BACK = "rolling_back"
ROLLED_BACK = "rolled_back"
FINALIZED = "finalized"      # 旧版本已下线，不可逆


@dataclass
class MigratorConfig:
    step_delay: float = 0.35          # 每个对象处理前的停顿，方便在 UI 观察
    max_retries: int = 3              # 单对象 CAS 冲突 / 注入失败的重试次数
    conflict_backoff: float = 0.25    # 冲突退避基数
    fail_on_names: tuple[str, ...] = ()
    fail_rate: float = 0.0            # 0~1，按概率注入失败


@dataclass
class _Attempt:
    name: str
    started_at: float
    finished_at: float | None = None
    ok: bool = False
    reason: str = ""
    attempts: int = 0


class StorageVersionMigrator:
    def __init__(
        self, definition: crd.CRDDefinition, store: EtcdStore, converter: Converter, bus
    ):
        self._crd = definition
        self._store = store
        self._converter = converter
        self._bus = bus
        self.config = MigratorConfig()
        self.phase = IDLE
        self._lock = threading.RLock()
        self._wake = threading.Condition(self._lock)
        self._stop = threading.Event()
        self._worker: threading.Thread | None = None
        self._run_id = itertools.count(1)
        self._current_run: int | None = None
        self.history: list[dict] = []

    # ============================================================== 状态
    def status(self) -> dict[str, Any]:
        with self._lock:
            candidates = self._store.get_migration_candidates()
            all_records = self._store.list()
            total = len(all_records)
            pending = len(candidates)
            return {
                "phase": self.phase,
                "storageVersion": self._crd.storage_version,
                "servedVersions": sorted(self._crd.served_versions),
                "total": total,
                "pending": pending,
                "migrated": total - pending,
                "config": {
                    "stepDelay": self.config.step_delay,
                    "maxRetries": self.config.max_retries,
                    "conflictBackoff": self.config.conflict_backoff,
                    "failOnNames": list(self.config.fail_on_names),
                    "failRate": self.config.fail_rate,
                },
                "workerRunning": self._worker is not None and self._worker.is_alive(),
                "history": list(self.history[-20:]),
            }

    def update_config(self, patch: dict) -> dict:
        with self._lock:
            if "stepDelay" in patch:
                self.config.step_delay = max(0.0, float(patch["stepDelay"]))
            if "maxRetries" in patch:
                self.config.max_retries = max(0, int(patch["maxRetries"]))
            if "conflictBackoff" in patch:
                self.config.conflict_backoff = max(0.0, float(patch["conflictBackoff"]))
            if "failOnNames" in patch:
                names = patch["failOnNames"]
                self.config.fail_on_names = tuple(names) if names is not None else ()
            if "failRate" in patch:
                rate = float(patch["failRate"])
                self.config.fail_rate = min(1.0, max(0.0, rate))
            self._bus.publish("migrator.config", self.status())
            return self.status()

    # ============================================================== 1. 翻转存储版本
    def flip_storage_version(self, target: str) -> dict:
        with self._lock:
            if self.phase == FINALIZED:
                raise RuntimeError("已经 finalize，存储版本不可再变更")
            if target == self._crd.storage_version:
                return self.status()
            self._crd.set_storage_version(target)
            pending = self._store.mark_all_pending(target)
            if target == crd.V1:
                # 正向迁移：首次开始、失败后重试、回滚后重新迁移都走这里
                self.phase = IN_PROGRESS if pending else SUCCEEDED
            else:
                # 回滚方向的翻转由 rollback() 负责，这里只记录状态
                self.phase = ROLLING_BACK if pending else ROLLED_BACK
            self._bus.publish(
                "migrator.flip",
                {
                    "storageVersion": target,
                    "pending": pending,
                    "phase": self.phase,
                },
            )
            self._kick_worker()
            return self.status()

    # ============================================================== 2. 后台迁移
    def start(self) -> dict:
        """显式启动/唤醒迁移 worker（翻转后通常已自动启动）。"""
        with self._lock:
            if self.phase not in (IN_PROGRESS, FAILED):
                raise RuntimeError(f"当前阶段 {self.phase} 不允许启动迁移")
            self.phase = IN_PROGRESS
            self._bus.publish("migrator.start", {"phase": self.phase})
            self._kick_worker()
            return self.status()

    def _kick_worker(self) -> None:
        if self._worker and self._worker.is_alive():
            self._wake.notify_all()
            return
        self._stop.clear()
        self._worker = threading.Thread(
            target=self._run, name="storage-migrator", daemon=True
        )
        self._worker.start()

    def shutdown(self) -> None:
        with self._lock:
            self._stop.set()
            self._wake.notify_all()
        if self._worker:
            self._worker.join(timeout=5)

    def _run(self) -> None:
        run_id = next(self._run_id)
        with self._lock:
            self._current_run = run_id
        try:
            while not self._stop.is_set():
                with self._lock:
                    if self.phase not in (IN_PROGRESS, ROLLING_BACK):
                        return
                    target_version = self._crd.storage_version
                    # failed 对象不在自动候选集里 —— 真实迁移器也要求人工
                    # 重新触发（retry_failed / start），避免错误风暴。
                    candidates = [
                        r for r in self._store.get_migration_candidates()
                        if r["migrationState"] in ("pending", "retrying")
                    ]
                    if not candidates:
                        self._finish_run()
                        return
                    record = candidates[0]
                    delay = self.config.step_delay
                if delay:
                    if self._stop.wait(timeout=delay):
                        return
                self._migrate_one(record, target_version, run_id)
        finally:
            with self._lock:
                if self._current_run == run_id:
                    self._current_run = None

    def _finish_run(self) -> None:
        failed = any(
            r["migrationState"] == "failed" for r in self._store.list()
        )
        if self.phase == ROLLING_BACK:
            self.phase = ROLLED_BACK
        elif failed:
            self.phase = FAILED
        else:
            self.phase = SUCCEEDED
        self._bus.publish("migrator.finish", {"phase": self.phase})

    def _migrate_one(self, record: dict, target_version: str, run_id: int) -> None:
        name = record["name"]
        attempt = _Attempt(name=name, started_at=time.time())
        self._store.mark_state(name, "migrating", {"runId": run_id})
        self._bus.publish("migrator.object.start", {"name": name})

        for try_index in range(self.config.max_retries + 1):
            attempt.attempts = try_index + 1
            try:
                fresh = self._store.get_stored(name)
                expected_rv = get_path(fresh, "metadata.resourceVersion")
                source_version = fresh["apiVersion"].split("/", 1)[1]

                # 失败注入：模拟 webhook / 数据问题。
                # 只在首次尝试触发 —— 对应真实的瞬时故障，退避重试后即可恢复；
                # 若要模拟持续故障，保持对象处于 failed 并人工决定何时重试。
                if try_index == 0 and self._should_fail(name):
                    raise RuntimeError(f"注入失败：{name} 的转换 webhook 暂时不可用")

                if source_version != target_version:
                    # read_view 已经实现完整语义：结构转换 + 把 stash 中属于
                    # 目标版本的独占字段叠回正式结构（字段「转正」）。
                    converted = self._converter.read_view(fresh, target_version)
                    # 重新整理 stash：
                    #  - 源版本独占字段从正式结构收进注解
                    #  - 已转正的目标版本条目移除
                    #  - 其余条目原样保留
                    import json

                    from .conversion import STASH_ANNOTATION

                    stash = self._converter._load_stash(fresh)
                    for path in crd.CRDDefinition.only_paths(source_version):
                        value = get_path(fresh, path, _MISSING_MARK)
                        if value is not _MISSING_MARK:
                            stash[path] = value
                    target_known = crd.CRDDefinition.known_paths(target_version)
                    stash = {
                        path: value
                        for path, value in stash.items()
                        if path not in target_known
                        or get_path(converted, path, _MISSING_MARK) != value
                    }
                    annotations = converted.get("metadata", {}).get("annotations", {})
                    if stash:
                        converted.setdefault("metadata", {})["annotations"] = {
                            **annotations, STASH_ANNOTATION:
                                json.dumps(stash, ensure_ascii=False, sort_keys=True)
                        }
                    else:
                        annotations.pop(STASH_ANNOTATION, None)
                        if annotations:
                            converted.setdefault("metadata", {})["annotations"] = annotations
                        else:
                            converted.get("metadata", {}).pop("annotations", None)
                else:
                    converted = fresh

                self._store.cas_rewrite(
                    name, expected_rv, converted,
                    reason=f"{source_version}->{target_version}",
                )
                self._store.mark_state(name, "migrated")
                attempt.ok = True
                attempt.finished_at = time.time()
                self._record_history(attempt, target_version)
                self._bus.publish(
                    "migrator.object.done",
                    {"name": name, "attempts": attempt.attempts},
                )
                return

            except ConflictError as exc:
                # 迁移期间对象被客户端改写：退避后重新读取、重新转换，绝不覆盖新写入
                wait = self.config.conflict_backoff * (2**try_index)
                self._store.mark_state(
                    name, "retrying",
                    {"conflict": True, "try": try_index + 1, "backoff": wait},
                )
                self._bus.publish(
                    "migrator.object.conflict",
                    {
                        "name": name,
                        "try": try_index + 1,
                        "expected": exc.expected,
                        "current": exc.current,
                        "backoff": wait,
                    },
                )
                if self._stop.wait(timeout=wait):
                    return

            except RuntimeError as exc:
                attempt.reason = str(exc)
                if try_index < self.config.max_retries:
                    wait = self.config.conflict_backoff * (2**try_index)
                    self._store.mark_state(
                        name, "retrying",
                        {"error": str(exc), "try": try_index + 1},
                    )
                    self._bus.publish(
                        "migrator.object.retry",
                        {"name": name, "try": try_index + 1, "error": str(exc)},
                    )
                    if self._stop.wait(timeout=wait):
                        return
                else:
                    break

        attempt.ok = False
        attempt.finished_at = time.time()
        self._store.mark_state(name, "failed", {"error": attempt.reason})
        self._record_history(attempt, target_version)
        self._bus.publish(
            "migrator.object.failed",
            {"name": name, "error": attempt.reason, "attempts": attempt.attempts},
        )

    def retry_failed(self) -> dict:
        with self._lock:
            if self.phase not in (FAILED, IN_PROGRESS):
                raise RuntimeError(f"当前阶段 {self.phase} 没有可重试的失败对象")
            for record in self._store.list():
                if record["migrationState"] == "failed":
                    self._store.mark_state(record["name"], "pending")
            self.phase = IN_PROGRESS
            self._bus.publish("migrator.retry", {})
            self._kick_worker()
            return self.status()

    # ============================================================== 3. 回滚
    def rollback(self) -> dict:
        """finalize 之前随时可回到旧控制器：storage 切回 v1alpha1，对象逐批写回。"""
        with self._lock:
            if self.phase == FINALIZED:
                raise RuntimeError("已经 finalize：旧版本已下线，无法回滚")
            if self.phase in (IDLE, ROLLED_BACK):
                return self.status()
            self._crd.set_storage_version(crd.V1ALPHA1)
            pending = self._store.mark_all_pending(crd.V1ALPHA1)
            self.phase = ROLLING_BACK
            self._bus.publish(
                "migrator.rollback.start",
                {"storageVersion": crd.V1ALPHA1, "pending": pending},
            )
            self._kick_worker()
            return self.status()

    # ============================================================== 4. finalize（不可逆）
    def finalize(self) -> dict:
        with self._lock:
            if self.phase == FINALIZED:
                return self.status()
            if self.phase not in (SUCCEEDED,):
                raise RuntimeError(
                    f"只有全部迁移成功（{SUCCEEDED}）才能 finalize，当前 {self.phase}"
                )
            # 数据丢失守卫。finalize 会下线旧版本，前提是当前所有 served 版本
            # 的客户端都仍能完整读回自己的数据：
            #  1. 没有未完成迁移的对象；
            #  2. 每个对象在每个 served 版本视图里，其独占字段一个都不缺
            #    （独占字段由 convertedObject 注解兜底，这正是它的职责）；
            #  3. stash 注解必须是可解析的 JSON，否则视为数据损坏。
            blocking = []
            for record in self._store.list():
                if record["migrationState"] != "migrated":
                    blocking.append({"name": record["name"], "reason": "尚未迁移完成"})
                    continue
                stored = record["stored"]
                try:
                    stash = self._converter._load_stash(stored)
                except Exception as exc:  # pragma: no cover - 防御性
                    blocking.append(
                        {"name": record["name"], "reason": f"convertedObject 注解损坏: {exc}"}
                    )
                    continue
                for version in crd.VERSIONS:
                    if not self._crd.is_served(version):
                        continue
                    view = self._converter.read_view(stored, version)
                    # 该版本的每条独占数据，无论在正式结构还是 stash 里，
                    # 视图都必须原样读回
                    for path in crd.CRDDefinition.only_paths(version):
                        authoritative = get_path(
                            stored, path, stash.get(path, _MISSING_MARK)
                        )
                        if authoritative is _MISSING_MARK:
                            continue
                        if get_path(view, path, _MISSING_MARK) != authoritative:
                            blocking.append(
                                {
                                    "name": record["name"],
                                    "reason": f"版本 {version} 独占字段 {path} 读回不一致",
                                }
                            )
            if blocking:
                self._bus.publish("migrator.finalize.blocked", {"blocking": blocking})
                raise RuntimeError(f"finalize 被数据安全守卫拦截: {blocking}")

            self._crd.decommission(crd.V1ALPHA1)
            self.phase = FINALIZED
            self._bus.publish(
                "migrator.finalize.done",
                {"servedVersions": sorted(self._crd.served_versions)},
            )
            return self.status()

    # ============================================================== 工具
    def _should_fail(self, name: str) -> bool:
        if name in self.config.fail_on_names:
            return True
        if self.config.fail_rate > 0:
            import random

            return random.random() < self.config.fail_rate
        return False

    def _record_history(self, attempt: _Attempt, target_version: str) -> None:
        self.history.append(
            {
                "name": attempt.name,
                "ok": attempt.ok,
                "attempts": attempt.attempts,
                "reason": attempt.reason,
                "targetVersion": target_version,
                "durationMs": int(
                    ((attempt.finished_at or time.time()) - attempt.started_at) * 1000
                ),
                "ts": attempt.finished_at or time.time(),
            }
        )
        self.history = self.history[-50:]
