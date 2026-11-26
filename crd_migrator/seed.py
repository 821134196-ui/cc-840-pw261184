"""演示用种子数据。"""
from __future__ import annotations

from . import crd


def seed_objects() -> list[dict]:
    """三个对象，覆盖不同的独占字段组合。"""
    base_meta = {
        "creationTimestamp": "2026-11-20T08:00:00Z",
    }
    return [
        {
            "apiVersion": crd.CRDDefinition.api_version(crd.V1ALPHA1),
            "kind": crd.KIND,
            "metadata": {"name": "edge-delivery-a", **base_meta},
            "spec": {
                "image": "registry.local/pointwise/worker:1.4.0",
                "replicas": 3,
                "retries": 5,
                "legacyTag": "canary-2026-q4",
                "upstream": {"legacyEndpoint": "http://legacy-svc:8088"},
                "env": {"REGION": "cn-east-1"},
            },
            "status": {"phase": "Running", "observedGeneration": 4, "legacyAck": "ACK-7731"},
        },
        {
            "apiVersion": crd.CRDDefinition.api_version(crd.V1ALPHA1),
            "kind": crd.KIND,
            "metadata": {"name": "edge-delivery-b", **base_meta},
            "spec": {
                "image": "registry.local/pointwise/worker:1.4.0",
                "replicas": 1,
                "retries": 2,
                "legacyTag": "stable",
                "env": {"REGION": "cn-north-2"},
            },
            "status": {"phase": "Pending", "observedGeneration": 1},
        },
        {
            "apiVersion": crd.CRDDefinition.api_version(crd.V1ALPHA1),
            "kind": crd.KIND,
            "metadata": {"name": "edge-delivery-c", **base_meta},
            "spec": {
                "image": "registry.local/pointwise/edge:2.0.1",
                "replicas": 2,
                "retries": 8,
                "legacyTag": "batch-job",
                "upstream": {"legacyEndpoint": "http://legacy-svc:8088"},
            },
            "status": {"phase": "Failed", "observedGeneration": 9, "legacyAck": "ACK-7790"},
        },
    ]
