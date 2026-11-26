"""整体装配：把 CRD、转换服务、存储、迁移器、客户端组装成一个模拟器。"""
from __future__ import annotations

import threading
from typing import Any

from . import crd, seed
from .conversion import Converter
from .events import EventBus
from .migrator import StorageVersionMigrator
from .simulator import BlastRunner, TypedClient
from .store import EtcdStore


class SimulationApp:
    def __init__(self) -> None:
        self.bus = EventBus()
        self.definition = crd.CRDDefinition(storage_version=crd.V1ALPHA1)
        self.converter = Converter()
        self.store = EtcdStore(self.definition, self.converter, self.bus)
        self.migrator = StorageVersionMigrator(
            self.definition, self.store, self.converter, self.bus
        )
        self.blast = BlastRunner(self.store, self.bus)
        self._lock = threading.RLock()
        self.reset()

    # ------------------------------------------------------------ 生命周期
    def reset(self) -> None:
        """恢复到初始状态：v1alpha1 存储、三个种子对象、迁移器 idle。"""
        with self._lock:
            self.migrator.shutdown()
            self.definition = crd.CRDDefinition(storage_version=crd.V1ALPHA1)
            self.store = EtcdStore(self.definition, self.converter, self.bus)
            self.migrator = StorageVersionMigrator(
                self.definition, self.store, self.converter, self.bus
            )
            self.blast = BlastRunner(self.store, self.bus)
            for obj in seed.seed_objects():
                self.store.create(obj)
            self.bus.publish("sim.reset", {"objects": self.store.snapshot()})

    # ------------------------------------------------------------ 客户端
    def client(self, version: str, name: str) -> TypedClient:
        return TypedClient(self.store, version, name)

    # ------------------------------------------------------------ 总览
    def overview(self) -> dict[str, Any]:
        return {
            "crd": self.definition.snapshot(),
            "migrator": self.migrator.status(),
            "store": self.store.snapshot(),
            "objects": [self.store.views(r["name"]) for r in self.store.list()],
        }
