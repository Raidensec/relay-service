from __future__ import annotations

import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

from relay_service.config import Settings
from relay_service.errors import RequestError
from relay_service.requests import authenticate, parse_event_request
from relay_service.store import SQLiteStore


def build_server(
    host: str, port: int, store: SQLiteStore, settings: Settings
) -> ThreadingHTTPServer:
    handler_type = make_handler(store, settings)
    return ThreadingHTTPServer((host, port), handler_type)


def make_handler(store: SQLiteStore, settings: Settings) -> type[BaseHTTPRequestHandler]:
    class RelayHandler(BaseHTTPRequestHandler):
        server_version = "relay-service/0.1"

        def do_GET(self) -> None:
            if urlsplit(self.path).path == "/health":
                store.check_ready()
                self.write_json(200, {"status": "ready"})
                return
            if not self.authorized():
                self.write_json(401, {"error": "unauthorized"})
                return
            event_id = urlsplit(self.path).path.removeprefix("/events/")
            if not event_id or "/" in event_id:
                self.write_json(404, {"error": "not found"})
                return
            events = store.inspect(event_id)
            if not events:
                self.write_json(404, {"error": "event not found"})
                return
            self.write_json(200, events[0])

        def do_POST(self) -> None:
            if urlsplit(self.path).path != "/events":
                self.write_json(404, {"error": "not found"})
                return
            if not self.authorized():
                self.write_json(401, {"error": "unauthorized"})
                return
            try:
                raw_body = self.read_body()
                parsed = parse_event_request(
                    {"x-caller-id": self.headers.get("X-Caller-ID", ""),
                     "idempotency-key": self.headers.get("Idempotency-Key", "")},
                    raw_body,
                    settings,
                )
                submission = store.submit(parsed, time.time())
            except RequestError as exc:
                self.write_json(exc.status, {"error": exc.message})
                return
            response = {
                "event_id": submission.event_id,
                "state": submission.state,
                "duplicate": submission.duplicate,
            }
            self.write_json(202, response)

        def authorized(self) -> bool:
            return authenticate(self.headers.get("Authorization"), settings.ingress_secret)

        def read_body(self) -> bytes:
            if self.headers.get_content_type() != "application/json":
                raise RequestError(415, "Content-Type must be application/json")
            try:
                length = int(self.headers.get("Content-Length", ""))
            except ValueError as exc:
                raise RequestError(400, "Content-Length must be a valid integer") from exc
            if length < 0:
                raise RequestError(400, "Content-Length cannot be negative")
            if length > settings.request_limit_bytes:
                raise RequestError(413, "request body is too large")
            raw_body = self.rfile.read(length)
            if len(raw_body) != length:
                raise RequestError(400, "request body ended before Content-Length")
            return raw_body

        def write_json(self, status: int, body: dict[str, object]) -> None:
            encoded = json.dumps(body, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:
            return

    return RelayHandler
