"""Core messaging structures and transport helpers for the Python client."""

from __future__ import annotations

import json
import logging
import queue
import random
import socket
import struct
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional


if TYPE_CHECKING:  # pragma: no cover
    from .EventSlice import EventSlice
    from Client import Client

logger = logging.getLogger(__name__)

ResponseCallback = Callable[["State"], None]

StatusQueue = queue.Queue
StatusQueueEmpty = queue.Empty


@dataclass
class Process:
    pid: int
    ttl: int = 3
    attempts: int = 2
    created: float = field(default_factory=time.time)
    updated: float = field(default_factory=time.time)
    data: "queue.Queue[object]" = field(default_factory=lambda: queue.Queue(maxsize=1))
    on_timeout: Optional[Callable[[], None]] = None

    def deliver(self, payload: object) -> None:
        self.updated = time.time()
        try:
            self.data.put_nowait(payload)
        except queue.Full:  # pragma: no cover - buffer is size 1 by design
            pass

    def await_data(self, timeout: float) -> object:
        self.updated = time.time()
        return self.data.get(timeout=timeout)


_processes: Dict[int, Process] = {}
_process_lock = threading.RLock()


def _register_process(process: Process) -> None:
    with _process_lock:
        _processes[process.pid] = process


def _remove_process(pid: int) -> Optional[Process]:
    with _process_lock:
        return _processes.pop(pid, None)


def _deliver_process(pid: int, payload: object) -> bool:
    with _process_lock:
        process = _processes.get(pid)
    if process is None:
        return False
    process.deliver(payload)
    _remove_process(pid)
    return True


def _read_exact(conn: "Client", length: int) -> bytes:
    buffer = bytearray()
    while len(buffer) < length:
        chunk = conn._conn.recv(length - len(buffer))
        if not chunk:
            raise EOFError("connection closed while reading")
        buffer.extend(chunk)
    return bytes(buffer)


def generate_pid() -> int:
    return random.randint(0, 999_999)


@dataclass
class Event:
    event: str
    destination: str
    data: Any
    origen: str = ""
    pid: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event": self.event,
            "destination": self.destination,
            "data": self.data,
            "origen": self.origen,
            "pid": self.pid,
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "Event":
        return cls(
            event=payload.get("event", ""),
            destination=payload.get("destination", ""),
            data=payload.get("data"),
            origen=payload.get("origen", ""),
            pid=int(payload.get("pid", 0)),
        )

    def send_data(
        self,
        client: Client,
        timeout: Optional[float] = None,
        callback: Optional[ResponseCallback] = None,
    ) -> Optional["State"]:
        self.pid = generate_pid()

        payload = json.dumps(self.to_dict()).encode("utf-8")
        message_size = len(payload)
        packet = struct.pack(">BI", 1, message_size) + payload
        process = Process(pid=self.pid)
        _register_process(process)

        try:
            with client._write_lock:
                client._conn.sendall(packet)
        except OSError as exc:
            _remove_process(self.pid)
            raise RuntimeError("error al enviar el mensaje") from exc

        effective_timeout = 15.0 if timeout is None else timeout
        print("Event sent, waiting for response...")  # Debug print
        if callback is None:
            threading.Thread(
                target=_fire_and_log,
                args=(process, self.pid, effective_timeout),
                daemon=True,
            ).start()
            return None

        try:
            data = process.await_data(effective_timeout)
        except queue.Empty as exc:
            _remove_process(self.pid)
            raise TimeoutError(f"timeout esperando respuesta para PID {self.pid}") from exc

        state = _ensure_state(data, self.pid)
        if state.status:
            callback(state)
            return state

        raise RuntimeError(f"error en la respuesta del servidor: {state.error}")


@dataclass
class State:
    data: Any
    pid: int=0
    status: bool = True
    message: str = ""
    error: str = ""
    destination: str=""
    origen: str=""
    

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "message": self.message,
            "error": self.error,
            "data": self.data,
            "destination": self.destination,
            "origen": self.origen,
            "pid": self.pid,
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "State":
        return cls(
            status=bool(payload.get("status", False)),
            message=str(payload.get("state", "")),
            error=str(payload.get("error", "")),
            data=payload.get("data"),
            destination=str(payload.get("destination", "")),
            origen=str(payload.get("origen", "")),
            pid=int(payload.get("pid", 0)),
        )

    def __str__(self) -> str:
        return (
            "State{Status: %s, Message: %r, Error: %r, Data: %r, Destination: %r, Origen: %r, PID: %d}"
            % (self.status, self.message, self.error, self.data, self.destination, self.origen, self.pid)
        )

    def send_data(self, client: Client, message_pid: int,dstination: str) -> None:
        ack = MessageState(
            status=True,
            server_pid=message_pid,
            message="El servidor proceso la solicitud",
            error="",
            process_status=1,
        )
        ack.send_data(client)

        self.destination = dstination
        self.pid = message_pid

        payload = json.dumps(self.to_dict()).encode("utf-8")
        message_size = len(payload)
        packet = struct.pack(">BI", 2, message_size) + payload
        try:
            with client._write_lock:
                client._conn.sendall(packet)
        except OSError as exc:
            logger.error("Error al enviar el mensaje: %s", exc)


@dataclass
class MessageState:
    status: bool
    server_pid: int
    message: str
    error: str
    process_status: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "server_pid": self.server_pid,
            "state": self.message,
            "error": self.error,
            "process_status": self.process_status,
        }

    def __str__(self) -> str:
        return (
            "MessageState{Status: %s, ServerPID: %d, Message: %r, Error: %r}"
            % (self.status, self.server_pid, self.message, self.error)
        )

    def send_data(self, client: "Client") -> None:
        payload = json.dumps(self.to_dict()).encode("utf-8")
        message_size = len(payload)
        packet = struct.pack(">BI", 3, message_size) + payload
        try:
            with client._write_lock:
                client._conn.sendall(packet)
        except OSError as exc:
            logger.error("Error al enviar el mensaje: %s", exc)


def _ensure_state(data: object, pid: int) -> State:
    if isinstance(data, State):
        if data.pid != pid:
            raise RuntimeError("PID incorrecto en la respuesta")
        return data
    if isinstance(data, dict):
        return State.from_dict(data)
    raise RuntimeError("Respuesta inesperada recibida")


def _fire_and_log(process: Process, pid: int, timeout: float) -> None:
    try:
        data = process.await_data(timeout)
        state = _ensure_state(data, pid)
        logger.info("Respuesta recibida: %s", state)
    except queue.Empty:
        logger.warning("Timeout esperando respuesta para PID %d", pid)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Respuesta inesperada para PID %d: %s", pid, exc)
    finally:
        _remove_process(pid)


def read_data(
    conn: "Client",
    client_name: str,
    event_slice: "EventSlice",
    server_status: "StatusQueue[bool]",
    latency: float,
) -> None:
    while True:
        try:
            header = _read_exact(conn, 9)
        except EOFError:
            logger.info("El servidor cerró la conexión")
            _notify_status(server_status, False)
            conn._conn.close()
            return
        except OSError as exc:
            logger.error("Error al leer encabezado, cerrando conexión: %s", exc)
            _notify_status(server_status, False)
            conn._conn.close()
            return

        msg_type = header[0]
        message_pid = struct.unpack(">I", header[1:5])[0]
        message_size = struct.unpack(">I", header[5:9])[0]

        try:
            payload = _read_exact(conn, message_size)
        except (EOFError, OSError) as exc:
            logger.error("Error al leer el mensaje: %s", exc)
            failure = MessageState(
                status=False,
                server_pid=message_pid,
                message="El servidor no puede procesar la solicitud",
                error=str(exc),
                process_status=2,
            )
            failure.send_data(conn)
            continue

        if msg_type == 1:  # Evento
            try:
                event_payload = json.loads(payload.decode("utf-8"))
                event = Event.from_dict(event_payload)
            except json.JSONDecodeError as exc:
                logger.error("Error al convertir el JSON: %s", exc)
                continue

            ack = MessageState(
                status=True,
                server_pid=message_pid,
                message="El servidor proceso la solicitud",
                error="",
                process_status=1,
            )
            ack.send_data(conn)

            try:
                from .Handlers import handle_events  # Local import to avoid circular dependency

                handle_events(event, conn, client_name, event_slice, int(latency), message_pid)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Error al procesar el evento %s: %s", event.event, exc)
        elif msg_type == 2:  # Estado
            try:
                state_payload = json.loads(payload.decode("utf-8"))
                state = State.from_dict(state_payload)
            except json.JSONDecodeError as exc:
                logger.error("Error al convertir el JSON: %s", exc)
                continue

            ack = MessageState(
                status=True,
                server_pid=message_pid,
                message="El servidor proceso la solicitud",
                error="",
                process_status=2,
            )
            ack.send_data(conn)

            if _deliver_process(state.pid, state):
                logger.info("Estado enviado al proceso con PID: %d", state.pid)
            else:
                logger.warning("No se encontró un proceso con PID: %d", state.pid)
                logger.info("Respuesta recibida: %s", state)
        else:
            logger.warning("Tipo de mensaje desconocido: %d", msg_type)


def _notify_status(queue_: "StatusQueue[bool]", status: bool) -> None:
    try:
        queue_.put_nowait(status)
    except queue.Full:  # pragma: no cover - queue size is unbounded by default
        pass


__all__ = [
    "Event",
    "State",
    "MessageState",
    "Process",
    "ResponseCallback",
    "read_data",
    "generate_pid",
    "StatusQueue",
    "StatusQueueEmpty",
]
