"""Shared fixtures: a small, configurable local HTTP server."""

from __future__ import annotations

import base64
import hashlib
import json
import re
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

RANGE_RE = re.compile(r"bytes=(\d*)-(\d*)")


def make_payload(size: int, seed: int = 13) -> bytes:
    """Deterministic pseudo-random payload (catches off-by-one range bugs)."""
    return bytes(((i * seed) + (i >> 8) * 31 + 7) % 256 for i in range(size))


@dataclass
class FileEntry:
    data: bytes
    allow_ranges: bool = True
    etag: str | None = None
    last_modified: str | None = "Wed, 21 Oct 2015 07:28:00 GMT"
    content_type: str = "application/octet-stream"
    content_disposition: str | None = None
    flip_etag_after: int | None = None
    fail_first: int = 0
    fail_status: int = 503
    delay: float = 0.0
    truncate_at: int | None = None
    extra_headers: dict[str, str] = field(default_factory=dict)


class TestServer:
    """Threaded HTTP server with per-path behaviours."""

    def __init__(self) -> None:
        self.files: dict[str, FileEntry] = {}
        self.hits: Counter[str] = Counter()
        self.ranges: list[tuple[str, int | None, int | None]] = []
        self.requests: list[dict[str, str]] = []
        self.auth: dict[str, tuple[str, str]] = {}
        self.bearer: dict[str, str] = {}
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self.lock = threading.Lock()

    # -- lifecycle -------------------------------------------------------- #
    def start(self) -> None:
        handler = _make_handler(self)

        class _QuietServer(ThreadingHTTPServer):
            def handle_error(self, request, client_address):  # noqa: D102, ANN001
                pass  # clients aborting mid-stream is normal in these tests

        self._httpd = _QuietServer(("127.0.0.1", 0), handler)
        self._httpd.daemon_threads = True
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        if self._thread is not None:
            self._thread.join(timeout=5)

    @property
    def port(self) -> int:
        assert self._httpd is not None
        return self._httpd.server_address[1]

    def url(self, path: str) -> str:
        return f"http://127.0.0.1:{self.port}/{path.lstrip('/')}"

    # -- content ---------------------------------------------------------- #
    def add(self, path: str, data: bytes, **kwargs) -> str:
        entry = FileEntry(data=data, **kwargs)
        if entry.etag is None:
            entry.etag = hashlib.sha256(data).hexdigest()[:16]
        self.files[path.lstrip("/")] = entry
        return self.url(path)


@pytest.fixture()
def server():
    srv = TestServer()
    srv.start()
    yield srv
    srv.stop()


@pytest.fixture()
def payload() -> bytes:
    return make_payload(2 * 1024 * 1024)


@pytest.fixture()
def outdir(tmp_path: Path) -> Path:
    target = tmp_path / "downloads"
    target.mkdir()
    return target


def _make_handler(srv: TestServer):  # noqa: C901
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):  # noqa: D102, ANN002
            return

        def do_HEAD(self):  # noqa: N802
            self._serve(head=True)

        def do_GET(self):  # noqa: N802
            self._serve(head=False)

        def do_POST(self):  # noqa: N802
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else b""
            payload = json.dumps(
                {"method": "POST", "body": body.decode("utf-8", "replace"), "headers": dict(self.headers)}
            ).encode()
            self._respond(200, {"Content-Type": "application/json"}, payload, head=False)

        # ------------------------------------------------------------------ #
        def _serve(self, *, head: bool) -> None:
            path = self.path.split("?", 1)[0].lstrip("/")
            with srv.lock:
                srv.hits[path] += 1
                hit = srv.hits[path]
            srv.requests.append(dict(self.headers.items()))

            if path == "echo":
                body = json.dumps({"headers": dict(self.headers), "path": path}).encode()
                self._respond(200, {"Content-Type": "application/json"}, body, head=head)
                return

            if path == "cookie/echo":
                body = (self.headers.get("Cookie") or "").encode()
                self._respond(200, {"Content-Type": "text/plain"}, body, head=head)
                return

            if path == "empty":
                self._respond(200, {"Content-Type": "text/plain", "ETag": '"e"'}, b"", head=head)
                return

            if path.startswith("redirect/"):
                target = "/" + path[len("redirect/") :]
                self.send_response(302)
                self.send_header("Location", target)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

            if path.startswith("flaky/"):
                parts = path.split("/", 3)
                failures, inner = int(parts[1]), parts[3] if len(parts) > 3 else parts[-1]
                if hit <= failures:
                    self._respond(503, {"Content-Type": "text/plain"}, b"try later", head=head)
                    return
                entry = srv.files.get(inner)
                if entry is None:
                    self._respond(404, {}, b"missing", head=head)
                    return
                self._serve_file(inner, entry, head=head)
                return

            if path.startswith("auth/"):
                inner = path[len("auth/") :]
                expected = srv.auth.get(inner)
                header = self.headers.get("Authorization") or ""
                if expected is None or not header.startswith("Basic "):
                    self._respond(
                        401, {"WWW-Authenticate": 'Basic realm="wget2"'}, b"auth required", head=head
                    )
                    return
                try:
                    decoded = base64.b64decode(header[6:]).decode()
                except Exception:  # pragma: no cover - malformed
                    decoded = ""
                if decoded != f"{expected[0]}:{expected[1]}":
                    self._respond(401, {}, b"bad credentials", head=head)
                    return
                data = srv.files.get(inner, FileEntry(b"authorized")).data
                self._respond(200, {"Content-Type": "text/plain"}, data, head=head)
                return

            if path.startswith("bearer/"):
                inner = path[len("bearer/") :]
                token = srv.bearer.get(inner)
                header = self.headers.get("Authorization") or ""
                if not token or header != f"Bearer {token}":
                    self._respond(401, {}, b"token required", head=head)
                    return
                data = srv.files.get(inner, FileEntry(b"tokenized")).data
                self._respond(200, {"Content-Type": "text/plain"}, data, head=head)
                return

            if path == "site/index.html":
                body = (
                    b"<html><body>"
                    b'<a href="/site/page1.html">one</a>'
                    b'<a href="page2.html">two</a>'
                    b'<a href="/site/skip.png">img</a>'
                    b"</body></html>"
                )
                self._respond(200, {"Content-Type": "text/html; charset=utf-8"}, body, head=head)
                return
            if path == "site/page1.html":
                body = b'<html><a href="/site/page3.html">three</a></html>'
                self._respond(200, {"Content-Type": "text/html"}, body, head=head)
                return
            if path in {"site/page2.html", "site/page3.html"}:
                self._respond(200, {"Content-Type": "text/html"}, b"<html>leaf</html>", head=head)
                return
            if path == "site/skip.png":
                self._respond(200, {"Content-Type": "image/png"}, b"\x89PNG\r\n", head=head)
                return

            entry = srv.files.get(path)
            if entry is None:
                self._respond(404, {"Content-Type": "text/plain"}, b"not found", head=head)
                return
            if entry.fail_first:
                with srv.lock:
                    failing = srv.hits[path] <= entry.fail_first
                if failing:
                    self._respond(entry.fail_status, {"Retry-After": "0"}, b"boom", head=head)
                    return
            if entry.delay:
                time.sleep(entry.delay)
            self._serve_file(path, entry, head=head)

        # ------------------------------------------------------------------ #
        def _serve_file(self, path: str, entry: FileEntry, *, head: bool) -> None:
            headers = {
                "Content-Type": entry.content_type,
                "Last-Modified": entry.last_modified or "",
                "ETag": entry.etag or "",
                "Accept-Ranges": "bytes" if entry.allow_ranges else "none",
            }
            headers.update(entry.extra_headers)
            if entry.content_disposition:
                headers["Content-Disposition"] = entry.content_disposition

            data = entry.data
            range_header = self.headers.get("Range")
            start: int | None = None
            end: int | None = None
            if range_header and entry.allow_ranges:
                match = RANGE_RE.search(range_header)
                if match:
                    raw_start, raw_end = match.groups()
                    if raw_start == "" and raw_end:
                        start, end = len(data) - int(raw_end), len(data) - 1
                    else:
                        start = int(raw_start)
                        end = int(raw_end) if raw_end else len(data) - 1
                    start = max(0, start)
                    end = min(end, len(data) - 1)
            with srv.lock:
                srv.ranges.append((path, start, end))

            if start is not None and end is not None and end >= start:
                body = data[start : end + 1]
                headers["Content-Range"] = f"bytes {start}-{end}/{len(data)}"
                self._respond(206, headers, body, head=head)
                return
            if entry.truncate_at is not None:
                headers["Content-Length"] = str(len(data))
                self.send_response(200)
                for name, value in headers.items():
                    if value != "":
                        self.send_header(name, value)
                self.end_headers()
                self.wfile.write(data[: entry.truncate_at])
                self.wfile.flush()
                self.close_connection = True
                return
            self._respond(200, headers, data, head=head)

        def _respond(self, status: int, headers: dict[str, str], body: bytes, *, head: bool) -> None:
            self.send_response(status)
            for name, value in headers.items():
                if value == "":
                    continue
                self.send_header(name, value)
            if "Content-Length" not in headers:
                self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if not head and body:
                try:
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):  # pragma: no cover
                    pass

    return Handler


@pytest.fixture()
def run_cli():
    """Invoke the CLI in-process and return the CliRunner result."""
    from typer.testing import CliRunner

    from wget2.cli import build_app

    runner = CliRunner()

    def _run(*args: str, config: dict | None = None):
        app = build_app(config or {})
        return runner.invoke(app, list(args), catch_exceptions=False)

    return _run
