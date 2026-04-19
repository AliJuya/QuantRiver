from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from queue import Queue


@dataclass(frozen=True)
class TFClosedEvent:
    tf: str
    candle_open_time: datetime


class EngineEventQueue(Queue[TFClosedEvent]):
    pass


def create_engine_event_queue(*, maxsize: int = 0) -> EngineEventQueue:
    """
    Unbounded by default so EventBridge can enqueue without blocking.
    """
    return EngineEventQueue(maxsize=maxsize)
