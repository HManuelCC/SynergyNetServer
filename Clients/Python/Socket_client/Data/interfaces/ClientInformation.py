"""System information helpers mirroring the Go client implementation."""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from typing import Dict

from .EventSlice import EventsSubscribed

try:
    import psutil
except ImportError as exc:  # pragma: no cover - surfaced at runtime
    raise RuntimeError("psutil is required for system statistics") from exc


def _safe_disk_io_counters() -> psutil._common.sdiskio:  # type: ignore[attr-defined]
    counters = psutil.disk_io_counters()
    if counters is None:  # pragma: no cover - psutil guarantees object on most systems
        raise RuntimeError("disk IO counters are unavailable")
    return counters


@dataclass
class ClientHardwareResourcesStatistics:
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    disk_usage: float = 0.0
    disk_busy: float = 0.0

    def get_system_stats(self) -> None:
        """Populate metrics using psutil, roughly matching the Go logic."""

        cpu_percent = psutil.cpu_percent(interval=None)
        self.cpu_usage = cpu_percent

        vm_stats = psutil.virtual_memory()
        self.memory_usage = vm_stats.percent

        disk_stats = psutil.disk_usage("/")
        self.disk_usage = disk_stats.percent

        io_start = _safe_disk_io_counters()
        time.sleep(1.0)
        io_end = _safe_disk_io_counters()

        read_delta = io_end.read_bytes - io_start.read_bytes
        write_delta = io_end.write_bytes - io_start.write_bytes
        total_io = read_delta + write_delta
        if total_io > 0:
            max_throughput = 100 * 1024 * 1024  # 100 MB/s heuristic
            busy = (total_io / max_throughput) * 100.0
            self.disk_busy = min(100.0, busy)
        else:
            self.disk_busy = 0.0

    def to_dict(self) -> Dict[str, float]:
        return asdict(self)


@dataclass
class ClientInformation:
    client_name: str
    latency: float
    resources: ClientHardwareResourcesStatistics
    events: EventsSubscribed

    def to_dict(self) -> Dict[str, object]:
        return {
            "client_name": self.client_name,
            "latency": self.latency,
            "resources": self.resources.to_dict(),
            "events": self.events.to_dict(),
        }

