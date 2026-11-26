"""CRD 版本转换核心。

对照真实 Kubernetes 的三件套：

1. **conversion webhook**（ConversionReview 协议）：API server 按目标版本
   调用 webhook 完成对象结构转换。这里用 :class:`Converter` 本地模拟。
2. **convertedObject 注解兜底**：当转换不是「无损往返」时，API server 把原始
   对象序列化后放进注解，避免独占字段在存储中丢失。本模拟器使用同样的思路，
   把非存储版本的独占字段以点路径为键收进 :data:`STASH_ANNOTATION`。
3. **读取合并 / 写入合并**：有类型的旧客户端 PUT 的对象天然不包含新版本字段，
   服务端必须以「存储中的现有对象」为底，再用请求体覆盖，而不是整对象替换。
"""
from __future__ import annotations

import copy
import json
from typing import Any

from . import crd

# 真实集群使用的是 "v1.crd.k8s.io/convertedObject"，这里加 sim 前缀表明是模拟器。
STASH_ANNOTATION = "sim.crd.k8s.io/converted-object"


class ConversionError(Exception):
    """转换失败（对应 webhook 返回 failed status，API server 回 422）。"""

    def __init__(self, message: str, field_path: str | None = None):
        super().__init__(message)
        self.field_path = field_path


# ---------------------------------------------------------------- 点路径工具
def get_path(obj: dict, path: str, default: Any = None) -> Any:
    cur: Any = obj
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def set_path(obj: dict, path: str, value: Any) -> None:
    cur = obj
    parts = path.split(".")
    for part in parts[:-1]:
        nxt = cur.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[part] = nxt
        cur = nxt
    cur[parts[-1]] = value


def delete_path(obj: dict, path: str) -> None:
    cur: Any = obj
    parts = path.split(".")
    for part in parts[:-1]:
        if not isinstance(cur, dict) or part not in cur:
            return
        cur = cur[part]
    if isinstance(cur, dict):
        cur.pop(parts[-1], None)


def deep_merge(base: dict, overlay: dict) -> None:
    """把 overlay 合并进 base（原地修改）。

    - dict 递归合并；list / 标量直接替换
    - 值为 None 表示删除该键（客户端显式置空的语义）
    """
    for key, value in overlay.items():
        if value is None:
            base.pop(key, None)
        elif isinstance(value, dict) and isinstance(base.get(key), dict):
            deep_merge(base[key], value)
        else:
            base[key] = value


def leaf_paths(obj: dict, prefix: str = "") -> list[str]:
    """列出对象中所有出现过的叶子点路径（用于 UI 高亮本次更新触及的字段）。"""
    out: list[str] = []
    for key, value in obj.items():
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict) and value:
            out.extend(leaf_paths(value, path))
        else:
            out.append(path)
    return out


def parse_api_version(api_version: str) -> tuple[str, str]:
    if "/" not in api_version:
        raise ConversionError(f"非法 apiVersion: {api_version!r}")
    group, version = api_version.split("/", 1)
    if group != crd.GROUP:
        raise ConversionError(f"apiVersion 组不匹配: {group!r} != {crd.GROUP!r}")
    if version not in crd.VERSIONS:
        raise ConversionError(f"不支持的版本: {version!r}")
    return group, version


class Converter:
    """无状态转换服务（一个进程里等价于一个 webhook 实例）。"""

    # -------------------------------------------------- 纯结构转换（webhook）
    def convert_structural(self, obj: dict, target_version: str) -> dict:
        """只做结构映射。源版本的独占字段在目标版本无对应，不会出现在结果里。

        它们的存活依赖存储层的 stash 机制（见 :meth:`write_merge`）。
        """
        if "apiVersion" not in obj:
            raise ConversionError("对象缺少 apiVersion")
        _, source_version = parse_api_version(obj["apiVersion"])
        if source_version == target_version:
            return copy.deepcopy(obj)

        out = {
            "apiVersion": crd.CRDDefinition.api_version(target_version),
            "kind": obj.get("kind", crd.KIND),
            "metadata": copy.deepcopy(obj.get("metadata", {})),
        }

        # 1) 同路径共享字段直接搬
        shared = crd.SHARED_SPEC_PATHS + crd.SHARED_STATUS_PATHS
        for path in shared:
            if get_path(obj, path, _MISSING) is not _MISSING:
                set_path(out, path, copy.deepcopy(get_path(obj, path)))

        # 2) 结构变化的字段走显式映射
        if source_version == crd.V1ALPHA1:
            mapping = crd.STRUCTURAL_MAP
        else:
            mapping = crd.BACK_MAP
        for src_path, dst_path in mapping.items():
            if get_path(obj, src_path, _MISSING) is not _MISSING:
                set_path(out, dst_path, copy.deepcopy(get_path(obj, src_path)))

        # 注意：源版本独占字段（legacyTag / timeoutSeconds ...）在此被丢弃，
        # 由 write_merge 负责收进 stash 注解。
        return out

    # --------------------------------------------------------- 读取视图
    def read_view(self, stored: dict, target_version: str) -> dict:
        """存储对象 -> 某版本客户端看到的视图。

        结构转换后，把 stash 里属于目标版本的独占字段叠回去，
        于是旧客户端能看到 legacyTag，新客户端能看到 timeoutSeconds。
        """
        _, stored_version = parse_api_version(stored["apiVersion"])
        view = self.convert_structural(stored, target_version)

        stash = self._load_stash(stored)
        if stash:
            known = crd.CRDDefinition.known_paths(target_version)
            for path, value in stash.items():
                if path in known:
                    set_path(view, path, copy.deepcopy(value))
        return view

    # --------------------------------------------------------- 写入合并
    def write_merge(
        self, stored: dict | None, incoming: dict, storage_version: str
    ) -> tuple[dict, dict]:
        """把客户端写请求合并进存储对象，返回 (新存储对象, 转换元信息)。

        这是「旧客户端更新对象不能擦掉新字段」的关键：
        入参先转成存储版本的骨架，再 deep-merge 到现有对象上；
        入参版本独有的字段单独收进 stash 注解，与存储字段互不覆盖。
        """
        _, incoming_version = parse_api_version(incoming["apiVersion"])
        skeleton = self.convert_structural(incoming, storage_version)
        skeleton.get("metadata", {}).pop("annotations", None)  # 注解由服务端管理

        if stored is not None:
            merged = copy.deepcopy(stored)
            # 客户端不允许改这些服务端字段
            skeleton_meta = skeleton.get("metadata", {})
            for protected in ("uid", "creationTimestamp", "generation", "resourceVersion"):
                skeleton_meta.pop(protected, None)
            deep_merge(merged, skeleton)
        else:
            merged = skeleton

        # 收集入参版本的独占字段（出现即记录，None 表示删除）
        stash = self._load_stash(stored) if stored else {}
        stash_before = copy.deepcopy(stash)
        incoming_only = set(crd.CRDDefinition.only_paths(incoming_version))
        touched_only_paths: list[str] = []
        for path in crd.CRDDefinition.known_paths(incoming_version):
            if get_path(incoming, path, _MISSING) is not _MISSING:
                value = get_path(incoming, path)
                if path in incoming_only:
                    touched_only_paths.append(path)
                    if value is None:
                        stash.pop(path, None)
                    else:
                        stash[path] = copy.deepcopy(value)

        # 修剪：只处理入参版本的独占路径 —— 且仅当当前存储版本的 schema
        # 正式支持该路径（或其结构映射落点）并有同值落点时，stash 副本才算冗余。
        # 存储版本不认识的路径必须留在 stash 里，等待存储版本迁移。
        stash, dropped = self._prune_stash(
            stash, merged, storage_version, incoming_only
        )

        if stash:
            merged.setdefault("metadata", {}).setdefault("annotations", {})[
                STASH_ANNOTATION
            ] = json.dumps(stash, ensure_ascii=False, sort_keys=True)
        else:
            ann = merged.get("metadata", {}).get("annotations")
            if isinstance(ann, dict):
                ann.pop(STASH_ANNOTATION, None)
                if not ann:
                    merged["metadata"].pop("annotations", None)

        meta = {
            "incomingVersion": incoming_version,
            "storageVersion": storage_version,
            "stashPaths": sorted(stash),
            "touchedOnlyPaths": touched_only_paths,
            "droppedStashPaths": dropped,
            # 只高亮业务字段；metadata 由服务端管理，不算是客户端触及的路径
            "changedPaths": [
                p for p in leaf_paths(incoming) if not p.startswith("metadata.")
            ],
            "stashChanged": stash != stash_before,
        }
        return merged, meta

    # --------------------------------------------------------- 无损往返检查
    def round_trip_check(self, obj: dict) -> list[str]:
        """检查对象转一圈再转回来是否丢字段（对应 CRD 的存储版本迁移前置校验）。"""
        version = parse_api_version(obj["apiVersion"])[1]
        other = crd.CRDDefinition.other_version(version)
        round_tripped = self.convert_structural(
            self.convert_structural(obj, other), version
        )
        lost: list[str] = []
        for path in crd.CRDDefinition.known_paths(version):
            original = get_path(obj, path, _MISSING)
            bounced = get_path(round_tripped, path, _MISSING)
            if original is not _MISSING and original != bounced:
                lost.append(path)
        return lost

    # --------------------------------------------------------- webhook 协议
    def serve_conversion_review(self, review: dict) -> dict:
        """模拟 admission/conversion webhook 的 ConversionReview 请求-响应。"""
        request = review.get("request", {})
        target = request.get("desiredAPIVersion", "")
        try:
            _, target_version = parse_api_version(target)
            converted = [
                self.convert_structural(obj, target_version)
                for obj in request.get("objects", [])
            ]
            return {
                "kind": "ConversionReview",
                "apiVersion": "apiextensions.k8s.io/v1",
                "response": {
                    "uid": request.get("uid", ""),
                    "result": {"status": "Success"},
                    "convertedObjects": converted,
                },
            }
        except ConversionError as exc:
            return {
                "kind": "ConversionReview",
                "apiVersion": "apiextensions.k8s.io/v1",
                "response": {
                    "uid": request.get("uid", ""),
                    "result": {
                        "status": "Failed",
                        "message": str(exc),
                        "details": {"field": exc.field_path or ""},
                    },
                },
            }

    # --------------------------------------------------------- 内部工具
    @staticmethod
    def _load_stash(obj: dict | None) -> dict[str, Any]:
        if not obj:
            return {}
        # 注解键是含点号的完整字符串，必须直接索引，不能走点路径工具
        annotations = obj.get("metadata", {}).get("annotations")
        if not isinstance(annotations, dict):
            return {}
        raw = annotations.get(STASH_ANNOTATION)
        if not raw:
            return {}
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _prune_stash(
        stash: dict[str, Any],
        merged: dict,
        storage_version: str,
        eligible_paths: set[str] | None = None,
    ) -> tuple[dict[str, Any], list[str]]:
        """删除 stash 中的冗余副本。

        修剪必须同时满足：
        1. 路径属于本次写入版本的独占路径（其他版本的条目无权动）；
        2. 当前存储版本的 schema 正式支持该路径，或存在结构映射落点
           —— 存储版本不认识的路径一旦丢掉，等存储版本迁移后就无处可寻；
        3. 存储对象里的正式值与 stash 值一致。
        """
        storage_known = crd.CRDDefinition.known_paths(storage_version)
        kept: dict[str, Any] = {}
        dropped: list[str] = []
        for path, value in stash.items():
            if eligible_paths is not None and path not in eligible_paths:
                kept[path] = value
                continue
            if value is None:
                dropped.append(path)
                continue

            supported = path in storage_known
            mapped_target: str | None = None
            if path in crd.STRUCTURAL_MAP:
                mapped_target = crd.STRUCTURAL_MAP[path]
            elif path in crd.BACK_MAP:
                mapped_target = crd.BACK_MAP[path]
            if not supported and mapped_target is None:
                kept[path] = value  # 存储版本无法正式表达，必须保留
                continue

            if supported and get_path(merged, path, _MISSING) == value:
                dropped.append(path)
                continue
            if (
                mapped_target is not None
                and get_path(merged, mapped_target, _MISSING) == value
            ):
                dropped.append(path)
                continue
            kept[path] = value
        return kept, dropped


class _Missing:  # 哨兵：区分「字段不存在」和「字段值为 None」
    def __repr__(self) -> str:
        return "<MISSING>"


_MISSING = _Missing()
