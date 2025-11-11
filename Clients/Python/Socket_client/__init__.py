"""Python port of the SynergyNet client."""

from .Data.interfaces.EventSlice import EventSlice as _EventSlice

# Shared event registry mirroring the Go package-level variable.
EventSlice = _EventSlice()

from .Client import Client  # noqa: E402  (import after EventSlice creation to avoid cycles)

__all__ = ["Client", "EventSlice","EventSliceInstance"]
