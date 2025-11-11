"""Python demo replicating the Go SynergyNet client behaviour."""

from __future__ import annotations

import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qs, urlparse

import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Socket_client import Client as SynergyClient
from Socket_client import EventSlice
from Socket_client.Data.interfaces.Event_State import Event, State

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_events() -> None:
    EventSlice.add_event("login", login_event_handler)
    EventSlice.add_event("registro", registro_event_handler)


def login_event_handler(event: Event, conn, message_pid: int,destination: str) -> None:
    print(destination)
    state = State(
        status=False,
        message="Hola go",
        error="",
        data=None,
        pid=event.pid,
    )
    logger.info("Mensaje recibido: %s", event.origen)
    state.send_data(conn, message_pid,destination)


def registro_event_handler(event: Event, conn, message_pid: int,destination: str) -> None:
    state = State(
        status=True,
        message="Hola amigo",
        error="",
        data=None,
        pid=event.pid,
    )
    logger.info("Mensaje recibido: %s Evento: %s", event.origen, event.event)
    state.send_data(conn, message_pid,destination)


def make_handler(client: SynergyClient) -> Callable[..., BaseHTTPRequestHandler]:
    class RequestHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - required by BaseHTTPRequestHandler
            parsed = urlparse(self.path)
            if parsed.path != "/login_prueba":
                self.send_error(404, "Not Found")
                return

            params = parse_qs(parsed.query)
            username = params.get("username", [""])[0]

            event = Event(
                event="login",
                destination="test_go",
                data={"username": username, "password": "test_password"},
            )

            def callback(response: State) -> None:
                print("Contesto del servidor recibido")
                logger.info("Respuesta del evento de login: %s", username)
                payload = json.dumps(response.to_dict()).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            try:
                client.send(event, callback=callback)
            except TimeoutError:
                self.send_error(504, "Timeout esperando respuesta del servidor")
                sys.exit(1)
            except RuntimeError as exc:
                logger.error("Error sending login event: %s", exc)
                self.send_error(500, "Internal Server Error")
                sys.exit(1)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003 - matches base class signature
            logger.info("HTTP %s - %s", self.address_string(), format % args)

    return RequestHandler


def create_routes(client: SynergyClient) -> None:
    server = ThreadingHTTPServer(("0.0.0.0", 8080), make_handler(client))
    logger.info("Servidor HTTP escuchando en :8080")
    try:
        server.serve_forever()
    except KeyboardInterrupt:  # pragma: no cover - convenience for manual runs
        logger.info("Servidor HTTP detenido por el usuario")
    finally:
        server.server_close()


def main() -> None:
    create_events()
    client = SynergyClient("localhost", "4430", "test_py", None, use_tls=False)

    try:
        create_routes(client)
    finally:
        client.close()


if __name__ == "__main__":
    main()
