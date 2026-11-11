#!/usr/bin/env python3
"""Tiny range-capable HTTP server for smoke-testing wget2 locally.

    python examples/demo_server.py --size 32M --port 8765
    wget2 -c 8 -C http://127.0.0.1:8765/demo.bin -P /tmp/demo

Features needed to exercise wget2: byte ranges, ETag/Last-Modified, redirects,
transient 503s (``/flaky/<n>/<name>``) and a server without range support
(``/norange/<name>``).
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

RANGE_RE = re.compile(r"bytes=(\d*)-(\d*)")

PAYLOAD = b""
ETAG = ""
LAST_MODIFIED = "Wed, 21 Oct 2015 07:28:00 GMT"
FAILURES: dict[str, int] = {}


def build_payload(size: int) -> bytes:
    block = bytes(range(256))
    return (block * (size // len(block) + 1))[:size]


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "wget2-demo/1.0"

    def log_message(self, fmt: str, *args) -> None:  # noqa: ANN002
        sys.stderr.write(f"  {self.command} {self.path} -> {args[1]}\n")

    def do_HEAD(self) -> None:  # noqa: N802
        self._serve(head=True)

    def do_GET(self) -> None:  # noqa: N802
        self._serve(head=False)

    def _serve(self, *, head: bool) -> None:
        path = self.path.split("?", 1)[0].lstrip("/")
        if path.startswith("redirect/"):
            self.send_response(302)
            self.send_header("Location", "/" + path[len("redirect/") :])
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        allow_ranges = True
        if path.startswith("norange/"):
            allow_ranges = False
            path = path[len("norange/") :]
        if path.startswith("flaky/"):
            _, count, path = path.split("/", 2)
            FAILURES[path] = FAILURES.get(path, 0) + 1
            if FAILURES[path] <= int(count):
                body = b"try again later\n"
                self.send_response(503)
                self.send_header("Retry-After", "0")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if not head:
                    self.wfile.write(body)
                return

        data = PAYLOAD
        headers = {
            "Content-Type": "application/octet-stream",
            "ETag": ETAG,
            "Last-Modified": LAST_MODIFIED,
            "Accept-Ranges": "bytes" if allow_ranges else "none",
        }
        start = end = None
        range_header = self.headers.get("Range")
        if range_header and allow_ranges:
            match = RANGE_RE.search(range_header)
            if match:
                raw_start, raw_end = match.groups()
                start = int(raw_start) if raw_start else 0
                end = int(raw_end) if raw_end else len(data) - 1
                end = min(end, len(data) - 1)

        if start is not None and end is not None:
            body = data[start : end + 1]
            headers["Content-Range"] = f"bytes {start}-{end}/{len(data)}"
            self.send_response(206)
        else:
            body = data
            self.send_response(200)
        for name, value in headers.items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if head:
            return
        try:
            # small chunks + tiny sleeps so progress output is visible
            for offset in range(0, len(body), 64 * 1024):
                self.wfile.write(body[offset : offset + 64 * 1024])
                time.sleep(0.001)
        except (BrokenPipeError, ConnectionResetError):
            pass


def main() -> int:
    global PAYLOAD, ETAG
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--size", default="32M", help="payload size, e.g. 32M")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--name", default="demo.bin", help="served filename")
    args = parser.parse_args()

    suffix = args.size.strip()[-1].upper()
    scale = {"K": 1024, "M": 1024**2, "G": 1024**3}.get(suffix, 1)
    size = int(args.size.strip().rstrip("KMGBkmgb") or 0) * scale
    PAYLOAD = build_payload(size)
    ETAG = f'"{hashlib.sha256(PAYLOAD[:4096]).hexdigest()[:16]}"'

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    server.daemon_threads = True
    print(f"serving {size / 1024 / 1024:.1f} MiB on http://{args.host}:{args.port}/{args.name}")
    print(f"  plain        http://{args.host}:{args.port}/{args.name}")
    print(f"  no ranges    http://{args.host}:{args.port}/norange/{args.name}")
    print(f"  flaky        http://{args.host}:{args.port}/flaky/2/{args.name}")
    print(f"  redirect     http://{args.host}:{args.port}/redirect/{args.name}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nbye")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
