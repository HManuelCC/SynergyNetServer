"""Python port of the SynergyNet client."""

from .Data.interfaces.EventSlice import EventSlice as _EventSlice
from synergynet_py.Data.interfaces.Event_State import Event, State

# Shared event registry mirroring the Go package-level variable.
EventSlice = _EventSlice()

from .Client import Client  # noqa: E402  (import after EventSlice creation to avoid cycles)

__all__ = ["Client", "EventSlice","EventSliceInstance", "Event", "State"]
