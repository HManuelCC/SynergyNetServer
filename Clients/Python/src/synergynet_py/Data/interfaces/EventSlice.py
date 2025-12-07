"""Event registry mirroring the Go EventSlice implementation."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Dict, Iterable, List

try:
    import socket
except ImportError:  # pragma: no cover
    socket = None  # type: ignore

if socket is None:  # pragma: no cover
    raise RuntimeError("socket module is required")

if TYPE_CHECKING:  # pragma: no cover
    from .Event_State import Event
    from Client import Client


EventHandler = Callable[["Event", "Client", int,str], None]

@dataclass
class EventsSubscribed:
    events: list[str]
    def to_dict(self) -> Dict[str, list[str]]:
        return {"events": self.events}

EventSliceInstance = EventsSubscribed(events=[])

@dataclass(frozen=True)
class EventString:
    name: str
    event_process: EventHandler


class EventSlice:
    """Thread-safe container for custom event handlers."""

    def __init__(self) -> None:
        self._events: List[EventString] = []
        self._lock = threading.RLock()

    def add_event(self, name: str, handler: EventHandler) -> None:
        with self._lock:
            self._events.append(EventString(name=name, event_process=handler))
            EventSliceInstance.events.append(name)

    def remove_event(self, name: str) -> None:
        with self._lock:
            self._events = [evt for evt in self._events if evt.name != name]
            EventSliceInstance.events.remove(name)

    def __len__(self) -> int:
        with self._lock:
            return len(self._events)

    def __iter__(self) -> Iterable[EventString]:
        with self._lock:
            snapshot = list(self._events)
        return iter(snapshot)


__all__ = ["EventSlice", "EventString", "EventHandler"]
