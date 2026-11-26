"""模拟 etcd 的存储层。

与真实 etcd / Kubernetes API 一致的关键语义：
- 全局单调 resourceVersion，每次写操作 +1
- 写请求携带 resourceVersion 做乐观并发控制（CAS），不匹配返回 409 Conflict
- 存储里永远只有「存储版本」格式的对象；其他版本字段以 stash 注解存活
- 对象额外带迁移状态：migrated=false 表示它的存储格式还停留在旧版本
"""
from __future__ import annotations

import copy
import itertools
import threading
import time

from . import crd
from .conversion import Converter, STASH_ANNOTATION, get_path, set_path


class ConflictError(Exception):
    """resourceVersion 不匹配（HTTP 409）。"""

    def __init__(self, name: str, expected: str, current: str):
        super().__init__(
            f"对象 {name!r} 已被其他写入更新: 期望 rv={expected}, 实际 rv={current}"
        )
        self.name = name
        self.expected = expected
        self.current = current


class NotFoundError(Exception):
    """对象不存在（HTTP 404）。"""


class VersionNotServedError(Exception):
    """请求的版本已被下线（HTTP 410）。"""


class EtcdStore:
    def __init__(self, definition: crd.CRDDefinition, converter: Converter, bus):
        self._crd = definition
        self._converter = converter
        self._bus = bus
        self._lock = threading.RLock()
        self._records: dict[str, dict] = {}
        self._rv = itertools.count(1)
        self._uid_seq = itertools.count(1)

    # ============================================================== 内部工具
    def _next_rv(self) -> str:
        return str(next(self._rv))

    def _record(self, name: str) -> dict:
        try:
            return self._records[name]
        except KeyError:
            raise NotFoundError(f"对象 {name!r} 不存在") from None

    def _check_served(self, version: str) -> None:
        if not self._crd.is_served(version):
            raise VersionNotServedError(
                f"版本 {version!r} 已被下线（finalize 后不可再服务该版本）"
            )

    # ============================================================== 读
    def list(self) -> list[dict]:
        with self._lock:
            return [copy.deepcopy(r) for r in self._records.values()]

    def get_stored(self, name: str) -> dict:
        with self._lock:
            return copy.deepcopy(self._record(name)["stored"])

    def get_view(self, name: str, version: str) -> dict:
        self._check_served(version)
        with self._lock:
            record = self._record(name)
            view = self._converter.read_view(record["stored"], version)
            set_path(view, "metadata.resourceVersion", record["resourceVersion"])
            return view

    def views(self, name: str) -> dict:
        """一次给出磁盘格式和两个客户端视图，供迁移台三栏对照。"""
        with self._lock:
            record = self._record(name)
            out = {
                "name": name,
                "migrationState": record["migrationState"],
                "resourceVersion": record["resourceVersion"],
                "stored": copy.deepcopy(record["stored"]),
                "stash": self._converter._load_stash(record["stored"]),
                "views": {},
            }
            for version in crd.VERSIONS:
                if self._crd.is_served(version):
                    out["views"][version] = self.get_view(name, version)
                else:
                    out["views"][version] = None
            return out

    # ============================================================== 写
    def create(self, incoming: dict) -> tuple[dict, dict]:
        from .conversion import parse_api_version

        version = parse_api_version(incoming["apiVersion"])[1]
        self._check_served(version)
        name = get_path(incoming, "metadata.name")
        if not name:
            raise ValueError("metadata.name 必填")

        with self._lock:
            if name in self._records:
                raise ConflictError(name, "<不存在>", self._records[name]["resourceVersion"])

            storage_version = self._crd.storage_version
            merged, meta = self._converter.write_merge(None, incoming, storage_version)
            now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            uid = f"uid-{next(self._uid_seq):04d}"
            merged["metadata"].update(
                {
                    "name": name,
                    "uid": uid,
                    "creationTimestamp": now,
                    "generation": 1,
                    "resourceVersion": self._next_rv(),
                }
            )
            record = {
                "name": name,
                "uid": uid,
                "resourceVersion": merged["metadata"]["resourceVersion"],
                "stored": merged,
                "migrationState": (
                    "migrated" if storage_version == crd.V1 else "pending"
                ),
                "lastWriteMeta": meta,
                "lastWriteAt": time.time(),
            }
            self._records[name] = record
            self._bus.publish(
                "store.create",
                {
                    "name": name,
                    "version": version,
                    "resourceVersion": record["resourceVersion"],
                    "meta": meta,
                },
            )
            return copy.deepcopy(merged), meta

    def update(
        self, incoming: dict, expected_rv: str | None = None
    ) -> tuple[dict, dict]:
        from .conversion import parse_api_version

        version = parse_api_version(incoming["apiVersion"])[1]
        self._check_served(version)
        name = get_path(incoming, "metadata.name")
        if not name:
            raise ValueError("metadata.name 必填")

        with self._lock:
            record = self._record(name)
            current_rv = record["resourceVersion"]
            if expected_rv is not None and expected_rv != current_rv:
                self._bus.publish(
                    "store.conflict",
                    {
                        "name": name,
                        "version": version,
                        "expected": expected_rv,
                        "current": current_rv,
                    },
                )
                raise ConflictError(name, expected_rv, current_rv)

            storage_version = self._crd.storage_version
            merged, meta = self._converter.write_merge(
                record["stored"], incoming, storage_version
            )
            # 服务端字段保持权威
            merged["metadata"]["uid"] = record["uid"]
            merged["metadata"]["creationTimestamp"] = get_path(
                record["stored"], "metadata.creationTimestamp"
            )
            old_generation = get_path(record["stored"], "metadata.generation", 1)
            merged["metadata"]["generation"] = old_generation
            if self._spec_changed(record["stored"], merged):
                merged["metadata"]["generation"] = old_generation + 1
            new_rv = self._next_rv()
            merged["metadata"]["resourceVersion"] = new_rv

            record["stored"] = merged
            record["resourceVersion"] = new_rv
            record["lastWriteMeta"] = meta
            record["lastWriteAt"] = time.time()
            self._bus.publish(
                "store.update",
                {
                    "name": name,
                    "version": version,
                    "resourceVersion": new_rv,
                    "meta": meta,
                },
            )
            return copy.deepcopy(merged), meta

    def cas_rewrite(
        self, name: str, expected_rv: str, new_stored: dict, reason: str
    ) -> dict:
        """迁移器专用：已完成版本转换的整对象 CAS 重写，不经过客户端合并。"""
        with self._lock:
            record = self._record(name)
            if record["resourceVersion"] != expected_rv:
                raise ConflictError(name, expected_rv, record["resourceVersion"])
            new_rv = self._next_rv()
            new_stored = copy.deepcopy(new_stored)
            new_stored.setdefault("metadata", {})["resourceVersion"] = new_rv
            record["stored"] = new_stored
            record["resourceVersion"] = new_rv
            record["lastWriteAt"] = time.time()
            record["lastWriteMeta"] = {"reason": reason, "actor": "migrator"}
            self._bus.publish(
                "store.rewrite",
                {"name": name, "reason": reason, "resourceVersion": new_rv},
            )
            return copy.deepcopy(record)

    def patch_spec(
        self, name: str, version: str, patch: dict, expected_rv: str | None = None
    ) -> tuple[dict, dict]:
        """以某版本客户端视角做局部 patch（先读视图 -> 改 -> 整体 update）。"""
        view = self.get_view(name, version)
        spec = view.setdefault("spec", {})
        for key, value in patch.items():
            spec[key] = value
        return self.update(view, expected_rv=expected_rv)

    def delete(self, name: str) -> None:
        with self._lock:
            self._record(name)
            del self._records[name]
            self._bus.publish("store.delete", {"name": name})

    # ============================================================== 迁移辅助
    def mark_all_pending(self, storage_version: str) -> list[str]:
        """存储版本翻转后，把与新存储版本格式不一致的对象标为待迁移。"""
        changed: list[str] = []
        with self._lock:
            for record in self._records.values():
                stored_version = record["stored"]["apiVersion"].split("/", 1)[1]
                if stored_version != storage_version:
                    record["migrationState"] = "pending"
                    changed.append(record["name"])
        return changed

    def mark_state(self, name: str, state: str, detail: dict | None = None) -> None:
        with self._lock:
            record = self._record(name)
            record["migrationState"] = state
            if detail is not None:
                record["migrationDetail"] = detail

    def get_migration_candidates(self) -> list[dict]:
        with self._lock:
            return [
                copy.deepcopy(r)
                for r in self._records.values()
                if r["migrationState"] in ("pending", "failed", "retrying")
            ]

    def reset(self) -> None:
        with self._lock:
            self._records.clear()

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "objectCount": len(self._records),
                "objects": [
                    {
                        "name": r["name"],
                        "resourceVersion": r["resourceVersion"],
                        "migrationState": r["migrationState"],
                        "storedVersion": r["stored"]["apiVersion"].split("/", 1)[1],
                        "hasStash": STASH_ANNOTATION
                        in r["stored"].get("metadata", {}).get("annotations", {}),
                    }
                    for r in self._records.values()
                ],
            }

    @staticmethod
    def _spec_changed(old: dict, new: dict) -> bool:
        return old.get("spec") != new.get("spec")
