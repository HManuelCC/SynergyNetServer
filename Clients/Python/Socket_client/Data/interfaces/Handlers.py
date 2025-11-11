"""Event handlers replicated from the Go implementation."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from .ClientInformation import ClientHardwareResourcesStatistics, ClientInformation
from .EventSlice import EventSlice, EventSliceInstance
from .Event_State import Event, State

if TYPE_CHECKING:  # pragma: no cover
    import socket
    from Client import Client

logger = logging.getLogger(__name__)


def handle_events(
    event: Event,
    conn: "Client",
    client_name: str,
    event_slice: EventSlice,
    latency_ms: int,
    message_pid: int,
) -> None:
    """Dispatch events just like the Go counterpart."""

    if event.event == "connect":
        stats = ClientHardwareResourcesStatistics()
        try:
            stats.get_system_stats()
        except Exception as exc:  # noqa: BLE001
            logger.error("error gathering system stats: %s", exc)
            state = State(
                status=False,
                message="Error al convertir la información del cliente a JSON",
                error=str(exc),
                data=None,
                destination="127.0.0.1",
                origen=client_name,
                pid=event.pid,
            )
            state.send_data(conn, message_pid, "127.0.0.1")
            return

        client_info = ClientInformation(
            client_name=client_name,
            latency=float(latency_ms),
            resources=stats,
            events=EventSliceInstance,
        )
        payload = json.dumps(client_info.to_dict())
        state = State(
            status=True,
            message="Cliente conectado con exito.",
            error="",
            data=payload,
            destination="127.0.0.1",
            origen=client_name,
            pid=event.pid,
        )
        state.send_data(conn, message_pid,"127.0.0.1")
        return

    for registered in event_slice:
        if registered.name == event.event:
            registered.event_process(event, conn, message_pid,event.origen)
            return

    logger.warning("Evento no reconocido: pid=%d", message_pid)
    state = State(
        status=False,
        message="No se puede reconocer el evento",
        error="Evento no reconocido",
        data=None,
        destination=event.origen,
        origen=event.destination,
        pid=event.pid,
    )
    state.send_data(conn, message_pid,event.origen)
