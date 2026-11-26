"""PointwiseJob CRD 的定义。

对应真实集群里的 CustomResourceDefinition 对象：
- 两个 served 版本 v1alpha1 / v1，其中恰好一个 storage=true
- 每个版本声明自己的字段集合，用于区分「本版本认识的字段」和「别的版本独占的字段」
"""
from __future__ import annotations

GROUP = "delivery.pointwise.io"
KIND = "PointwiseJob"
PLURAL = "pointwisejobs"

V1ALPHA1 = "v1alpha1"
V1 = "v1"
VERSIONS = (V1ALPHA1, V1)

# 两个版本都能识别、按同路径直接对应的字段（spec / status 分开记录）。
SHARED_SPEC_PATHS = (
    "spec.image",
    "spec.replicas",
    "spec.env",
)
SHARED_STATUS_PATHS = (
    "status.phase",
    "status.observedGeneration",
)

# 结构发生变化、需要显式映射的字段。
STRUCTURAL_MAP = {
    # v1alpha1 路径 -> v1 路径
    "spec.retries": "spec.retryPolicy.maxRetries",
}
BACK_MAP = {v: k for k, v in STRUCTURAL_MAP.items()}

# 各版本「只有自己认识」的独占字段（本演示的核心）。
V1ALPHA1_ONLY_PATHS = (
    "spec.legacyTag",
    "spec.upstream.legacyEndpoint",
    "status.legacyAck",
)
V1_ONLY_PATHS = (
    "spec.timeoutSeconds",
    "spec.retryPolicy.backoffSeconds",
    "spec.nodeSelector",
    "status.scheduledNode",
)

# 各版本客户端结构体里出现的全部字段路径（用于有类型客户端序列化时裁剪字段）。
V1ALPHA1_KNOWN = frozenset(
    SHARED_SPEC_PATHS
    + tuple(STRUCTURAL_MAP.keys())
    + V1ALPHA1_ONLY_PATHS
    + SHARED_STATUS_PATHS
)
V1_KNOWN = frozenset(
    SHARED_SPEC_PATHS
    + tuple(BACK_MAP.keys())
    + V1_ONLY_PATHS
    + SHARED_STATUS_PATHS
)


class CRDDefinition:
    """内存中的 CRD 描述，storage 版本可在迁移过程中切换。"""

    def __init__(self, storage_version: str = V1ALPHA1):
        if storage_version not in VERSIONS:
            raise ValueError(f"未知版本: {storage_version}")
        self._storage_version = storage_version
        # finalize 之后旧版本被下线，不再 served
        self.served_versions: set[str] = set(VERSIONS)

    # -- 版本标志 ----------------------------------------------------------
    @property
    def storage_version(self) -> str:
        return self._storage_version

    def set_storage_version(self, version: str) -> None:
        if version not in VERSIONS:
            raise ValueError(f"未知版本: {version}")
        self._storage_version = version

    def is_served(self, version: str) -> bool:
        return version in self.served_versions

    def decommission(self, version: str) -> None:
        """finalize：把旧版本从 served 列表移除（不可逆）。"""
        self.served_versions.discard(version)

    # -- 字段归属 ----------------------------------------------------------
    @staticmethod
    def known_paths(version: str) -> frozenset[str]:
        if version == V1ALPHA1:
            return V1ALPHA1_KNOWN
        if version == V1:
            return V1_KNOWN
        raise ValueError(f"未知版本: {version}")

    @staticmethod
    def only_paths(version: str) -> tuple[str, ...]:
        if version == V1ALPHA1:
            return V1ALPHA1_ONLY_PATHS
        if version == V1:
            return V1_ONLY_PATHS
        raise ValueError(f"未知版本: {version}")

    @staticmethod
    def api_version(version: str) -> str:
        return f"{GROUP}/{version}"

    @staticmethod
    def other_version(version: str) -> str:
        return V1 if version == V1ALPHA1 else V1ALPHA1

    def snapshot(self) -> dict:
        return {
            "group": GROUP,
            "kind": KIND,
            "plural": PLURAL,
            "versions": [
                {
                    "name": v,
                    "served": v in self.served_versions,
                    "storage": v == self._storage_version,
                }
                for v in VERSIONS
            ],
            "v1alpha1OnlyPaths": list(V1ALPHA1_ONLY_PATHS),
            "v1OnlyPaths": list(V1_ONLY_PATHS),
            "structuralMap": STRUCTURAL_MAP,
        }
