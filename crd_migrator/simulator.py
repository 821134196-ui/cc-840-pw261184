"""有类型的 API 客户端与并发更新驱动器。

TypedClient 模拟真实控制器里的 informer + lister + typed client：
- 读：只拿到本版本结构体认识的字段（服务端已做版本转换 + stash 叠加）
- 改：序列化回去时结构体里不存在的字段天然丢失 —— 字段保护完全由
  服务端 write_merge 负责，这正是本演示要验证的行为
"""
from __future__ import annotations

import copy
import threading
import time
from dataclasses import dataclass
from typing import Any

from . import crd
from .conversion import get_path, set_path, _MISSING
from .store import ConflictError, EtcdStore, NotFoundError, VersionNotServedError


class TypedClient:
    """固定版本的有类型客户端（v1alpha1 旧控制器 / v1 新控制器）。"""

    def __init__(self, store: EtcdStore, version: str, name: str):
        self.store = store
        self.version = version
        self.client_name = name

    def _api_version(self) -> str:
        return crd.CRDDefinition.api_version(self.version)

    def get(self, name: str) -> dict:
        view = self.store.get_view(name, self.version)
        # 模拟有类型结构体：只保留本版本 schema 里的路径，其余字段客户端「看不见」
        return self._trim_to_schema(view)

    def list(self) -> list[dict]:
        return [self.get(r["name"]) for r in self.store.list()]

    def _trim_to_schema(self, view: dict) -> dict:
        """模拟有类型结构体：spec/status 只保留本版本 schema 里的路径。

        metadata 不在 Go 结构体的版本化字段里 —— 真实客户端会把拿到的
        annotations/labels 原样带回，所以这里必须保留，否则 convertedObject
        注解会随旧客户端的一次普通更新被整体擦掉。
        """
        known = crd.CRDDefinition.known_paths(self.version)
        out: dict[str, Any] = {
            "apiVersion": view["apiVersion"],
            "kind": view.get("kind", crd.KIND),
            "metadata": {
                "name": get_path(view, "metadata.name"),
                "resourceVersion": get_path(view, "metadata.resourceVersion"),
                "generation": get_path(view, "metadata.generation"),
                "uid": get_path(view, "metadata.uid"),
            },
        }
        annotations = get_path(view, "metadata.annotations")
        if annotations:
            out["metadata"]["annotations"] = copy.deepcopy(annotations)
        labels = get_path(view, "metadata.labels")
        if labels:
            out["metadata"]["labels"] = copy.deepcopy(labels)
        for path in known:
            value = get_path(view, path, _MISSING)
            if value is not _MISSING:
                set_path(out, path, value)
        return out

    def update(
        self,
        name: str,
        mutate,
        *,
        max_attempts: int = 4,
        backoff: float = 0.2,
        before_write=None,
    ) -> dict:
        """读-改-写循环：遇到 409 冲突按指数退避重试（控制器的标准写法）。

        ``before_write`` 钩子在每次 get 之后、update 之前调用，用于模拟
        网络 RTT 之类的时间窗口（并发夹击测试靠它制造确定性的 CAS 冲突）。
        """
        last_error: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            obj = self.get(name)  # 每次重试都重新拿最新 rv
            mutate(obj)
            if before_write is not None:
                before_write(obj, attempt)
            obj["apiVersion"] = self._api_version()
            obj["kind"] = crd.KIND
            expected_rv = get_path(obj, "metadata.resourceVersion")
            try:
                stored, _meta = self.store.update(obj, expected_rv=expected_rv)
                self.store._bus.publish(
                    "client.update",
                    {
                        "client": self.client_name,
                        "version": self.version,
                        "name": name,
                        "attempt": attempt,
                        "result": "ok",
                    },
                )
                return stored
            except ConflictError as exc:
                last_error = exc
                self.store._bus.publish(
                    "client.update",
                    {
                        "client": self.client_name,
                        "version": self.version,
                        "name": name,
                        "attempt": attempt,
                        "result": "conflict",
                        "expected": exc.expected,
                        "current": exc.current,
                    },
                )
                if attempt == max_attempts:
                    break
                time.sleep(backoff * (2 ** (attempt - 1)))
        raise last_error  # type: ignore[misc]

    def patch(self, name: str, spec_patch: dict, **kwargs) -> dict:
        def mutate(obj: dict) -> None:
            for key, value in spec_patch.items():
                # 只允许写本版本认识的 spec 字段
                if f"spec.{key}" not in crd.CRDDefinition.known_paths(self.version):
                    raise ValueError(
                        f"{self.version} 客户端不认识字段 spec.{key}（会被结构体丢弃）"
                    )
                obj.setdefault("spec", {})[key] = value

        return self.update(name, mutate, **kwargs)

    def update_status(self, name: str, status_patch: dict, **kwargs) -> dict:
        def mutate(obj: dict) -> None:
            for key, value in status_patch.items():
                obj.setdefault("status", {})[key] = value

        return self.update(name, mutate, **kwargs)


# ---------------------------------------------------------------- 并发夹击
@dataclass
class BlastResult:
    client: str
    version: str
    name: str
    ok: bool
    attempts: int
    error: str = ""
    final_rv: str | None = None


class BlastRunner:
    """同时从旧/新两个版本发起更新，制造真实的 CAS 竞争。

    内存存储的临界区极短，两个线程即使同时起跑也会干净地串行化。
    真实系统里 get 与 update 之间隔着网络 RTT，窗口足以让另一个写入插进来 ——
    这里用 ``rtt`` 参数显式模拟这个窗口。
    """

    def __init__(self, store: EtcdStore, bus, rtt: float = 0.08):
        self.store = store
        self.bus = bus
        self.rtt = rtt

    def run(
        self,
        name: str,
        *,
        old_patch: dict | None = None,
        new_patch: dict | None = None,
        old_status: dict | None = None,
        new_status: dict | None = None,
    ) -> list[dict]:
        old_client = TypedClient(self.store, crd.V1ALPHA1, "legacy-controller")
        new_client = TypedClient(self.store, crd.V1, "nextgen-controller")

        results: list[BlastResult] = []
        barrier = threading.Barrier(2)
        self.bus.publish(
            "blast.start",
            {"name": name, "oldPatch": old_patch, "newPatch": new_patch, "rtt": self.rtt},
        )

        def worker(client: TypedClient, patch, status, label: str) -> None:
            barrier.wait()  # 两个线程同时发起，最大化冲突概率
            attempts_box = {"n": 0}

            def mutate(obj: dict) -> None:
                attempts_box["n"] += 1
                if patch:
                    for key, value in patch.items():
                        obj.setdefault("spec", {})[key] = value
                if status:
                    for key, value in status.items():
                        obj.setdefault("status", {})[key] = value

            try:
                stored = client.update(
                    name, mutate, max_attempts=5, backoff=0.15,
                    before_write=lambda obj, attempt: time.sleep(self.rtt),
                )
                results.append(
                    BlastResult(
                        client=label,
                        version=client.version,
                        name=name,
                        ok=True,
                        attempts=attempts_box["n"],
                        final_rv=get_path(stored, "metadata.resourceVersion"),
                    )
                )
            except (ConflictError, NotFoundError, VersionNotServedError) as exc:
                results.append(
                    BlastResult(
                        client=label,
                        version=client.version,
                        name=name,
                        ok=False,
                        attempts=attempts_box["n"],
                        error=str(exc),
                    )
                )

        t1 = threading.Thread(
            target=worker,
            args=(old_client, old_patch, old_status, "legacy-controller"),
            name="blast-old",
        )
        t2 = threading.Thread(
            target=worker,
            args=(new_client, new_patch, new_status, "nextgen-controller"),
            name="blast-new",
        )
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)

        payload = [r.__dict__ for r in results]
        self.bus.publish("blast.done", {"name": name, "results": payload})
        return payload
