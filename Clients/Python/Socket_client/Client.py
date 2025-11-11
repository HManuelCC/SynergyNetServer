"""Client implementation mirroring the Go SynergyNet client logic."""

from __future__ import annotations

import logging
import random
import socket
import ssl
import threading
import time
from dataclasses import dataclass
from typing import Optional

try:
    # Prefer absolute import when the package is installed or running as a top-level package.
    from SynergyNetClient.Socket_client import EventSlice as GLOBAL_EVENT_SLICE
except ImportError:
    # Fallback to local package when running the client standalone.
    from . import EventSlice as GLOBAL_EVENT_SLICE

from .Data.interfaces.EventSlice import EventSlice
from .Data.interfaces.Event_State import (
    Event,
    ResponseCallback,
    State,
    StatusQueue,
    StatusQueueEmpty,
    read_data,
)

logger = logging.getLogger(__name__)


@dataclass
class TLSConfig:
    """Minimal TLS configuration wrapper."""

    insecure_skip_verify: bool = True

    def to_ssl_context(self) -> ssl.SSLContext:
        context = ssl.create_default_context()
        if self.insecure_skip_verify:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        return context


class Client:
    """Maintains a resilient TLS connection and serialised writes."""

    def __init__(
        self,
        host: str,
        port: str,
        client_name: str,
        api_key: Optional[str] = None,
        use_tls: bool = True,
        event_slice: Optional[EventSlice] = None,
    ) -> None:
        self._host = host
        self._port = port
        self._name = client_name
        self._api_key = api_key
        self._use_tls = use_tls
        self._event_slice = event_slice or GLOBAL_EVENT_SLICE

        self._conn_lock = threading.RLock()
        self._conn: Optional[socket.socket] = None
        self._write_lock = threading.Lock()

        self._closed = threading.Event()
        self._done = threading.Event()
        self._attempt = 1

        self._min_backoff = 1.0
        self._max_backoff = 30.0

        self._tls_config = TLSConfig()
        self._thread = threading.Thread(target=self._run_tls if use_tls else self._run_tcp, daemon=True)
        self._thread.start()

    # ----------------------- public API -----------------------

    def send(
        self,
        event: Event,
        timeout: Optional[float] = None,
        callback: Optional[ResponseCallback] = None,
    ) -> Optional[State]:
        if self._closed.is_set():
            raise RuntimeError("client is closed")

        conn = self._get_conn()
        if conn is None:
            raise RuntimeError("no active connection")

        return event.send_data(self, timeout=timeout, callback=callback)

    def close(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        self._done.set()
        self._clear_conn()

    def connected(self) -> bool:
        return self._get_conn() is not None

    # ---------------------- internals -------------------------

    def _set_conn(self, conn: socket.socket) -> None:
        with self._conn_lock:
            self._conn = conn

    def _clear_conn(self) -> None:
        with self._conn_lock:
            if self._conn is not None:
                try:
                    self._conn.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                finally:
                    try:
                        self._conn.close()
                    except OSError:
                        pass
            self._conn = None

    def _get_conn(self) -> Optional[socket.socket]:
        with self._conn_lock:
            return self._conn

    def _run_tcp(self) -> None:
        # Plain TCP support kept for parity with the commented Go implementation.
        while not self._closed.is_set():
            try:
                start = time.time()
                conn = socket.create_connection((self._host, int(self._port)))
                latency_ms = int((time.time() - start) * 1_000)
                logger.info("[client %s] connected (latency %d ms)", self._name, latency_ms)
                self._attempt = 1
                self._set_conn(conn)
                self._handle_connection(self, latency_ms)
            except Exception as exc:  # noqa: BLE001
                delay = self._backoff()
                logger.warning(
                    "[client %s] connection error: %s (retry in %.1fs, attempt %d)",
                    self._name,
                    exc,
                    delay,
                    self._attempt,
                )
                if self._closed.wait(delay):
                    return
                self._attempt += 1

    def _run_tls(self) -> None:
        context = self._tls_config.to_ssl_context()
        while not self._closed.is_set():
            try:
                start = time.time()
                raw_sock = socket.create_connection((self._host, int(self._port)))
                conn = context.wrap_socket(raw_sock, server_hostname=self._host)
                latency_ms = int((time.time() - start) * 1_000)
                logger.info("[client %s] connected (latency %d ms)", self._name, latency_ms)
                self._attempt = 1
                self._set_conn(conn)
                self._handle_connection(self, latency_ms)
            except Exception as exc:  # noqa: BLE001
                delay = self._backoff()
                logger.warning(
                    "[client %s] TLS connection error: %s (retry in %.1fs, attempt %d)",
                    self._name,
                    exc,
                    delay,
                    self._attempt,
                )
                if self._closed.wait(delay):
                    return
                self._attempt += 1

    def _handle_connection(self, conn: socket.socket, latency_ms: int) -> None:
        server_status: StatusQueue = StatusQueue()
        reader = threading.Thread(
            target=read_data,
            args=(conn, self._name, self._event_slice, server_status, float(latency_ms)),
            daemon=True,
        )
        reader.start()
        try:
            while not self._closed.is_set():
                try:
                    status = server_status.get(timeout=0.5)
                    if not status:
                        logger.info("[client %s] disconnected by server, reconnecting", self._name)
                        break
                except StatusQueueEmpty:
                    continue
        finally:
            self._clear_conn()

    def _backoff(self) -> float:
        base = self._min_backoff * (2 ** (self._attempt - 1))
        base = min(base, self._max_backoff)
        jitter = random.uniform(-0.2 * base, 0.2 * base)
        return max(0.0, base + jitter)


__all__ = ["Client"]
