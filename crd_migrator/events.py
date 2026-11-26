"""进程内事件总线，供前端 SSE 订阅。"""
from __future__ import annotations

import itertools
import threading
import time
from collections import deque
from typing import Any

# 每个 SSE 连接最多回看的事件数，防止页面刷新后完全没有上下文。
HISTORY_LIMIT = 500


class EventBus:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._subscribers: set[Any] = set()
        self._seq = itertools.count(1)
        self._history: deque[dict] = deque(maxlen=HISTORY_LIMIT)

    def publish(self, event_type: str, message: dict | None = None, **extra: Any) -> dict:
        event = {
            "seq": next(self._seq),
            "ts": time.time(),
            "type": event_type,
            "message": message or {},
        }
        event["message"].update(extra)
        with self._cond:
            self._history.append(event)
            dead = []
            for queue in self._subscribers:
                try:
                    queue.append(event)
                except Exception:  # pragma: no cover - 防御性
                    dead.append(queue)
            for queue in dead:
                self._subscribers.discard(queue)
            self._cond.notify_all()
        return event

    def subscribe(self) -> "Subscription":
        return Subscription(self)

    def history_since(self, seq: int) -> list[dict]:
        with self._lock:
            return [e for e in self._history if e["seq"] > seq]


class Subscription:
    def __init__(self, bus: EventBus):
        self._bus = bus
        import queue

        self._queue: "queue.Queue[dict | None]" = queue.Queue()
        with bus._cond:
            bus._subscribers.add(self._queue)
            self._last_seen = len(bus._history)
            # 连接时先把已有历史投递一遍
            for event in list(bus._history):
                self._queue.put(event)

    def wait(self, timeout: float | None = None) -> dict | None:
        return self._queue.get(timeout=timeout)

    def close(self) -> None:
        with self._bus._cond:
            self._bus._subscribers.discard(self._queue)
        self._queue.put(None)
