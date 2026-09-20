"""wget2 — a multi-connection downloader with pluggable HTTP backends.

Backends
--------
requests     : ``requests`` (blocking, executed in worker threads)
pycurl       : ``pycurl`` (blocking, executed in worker threads)
pure         : stdlib ``urllib.request`` (blocking, executed in worker threads)
httpx        : sync ``httpx`` (blocking, executed in worker threads)
httpx-async  : async ``httpx`` (native asyncio) — **default**
curl         : ``curl`` via ``asyncio`` subprocess
aria2c       : ``aria2c`` via ``asyncio`` subprocess

All backends implement the same async :class:`Backend` protocol so the
download orchestrator is backend-agnostic.  Files larger than
``CHUNK_THRESHOLD`` are streamed straight into per-range ``.part`` files;
smaller files are buffered in memory and written out in one shot.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import shutil
import sys
import threading
import time
from collections.abc import Callable, Coroutine, Iterable, Sequence
from io import BytesIO
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CHUNK_THRESHOLD = 10 * 1024 * 1024  # > 10 MiB → stream parts to disk
STREAM_CHUNK = 1024 * 1024          # 1 MiB read chunks
PROGRESS_INTERVAL = 0.1             # seconds between progress redraws

Sink = Callable[[bytes], object]
Range = tuple[int, int]

HeadFn = Callable[[str], int]
FetchFn = Callable[[str, int, int, Sink, int], None]

# ---------------------------------------------------------------------------
# Backend protocol
# ---------------------------------------------------------------------------


class Backend(Protocol):
    """Minimal async interface every download backend must provide."""

    name: str

    async def head(self, url: str) -> int:
        """Return the ``Content-Length`` of *url* (following redirects)."""

    async def fetch_range(
        self,
        url: str,
        start: int,
        end: int,
        sink: Sink,
        chunk_size: int = STREAM_CHUNK,
    ) -> None:
        """Fetch inclusive byte range ``[start, end]`` and feed *sink*."""


class ThreadedBackend:
    """Adapts a pair of blocking functions to the async :class:`Backend` protocol."""

    def __init__(self, name: str, head_fn: HeadFn, fetch_fn: FetchFn) -> None:
        self.name = name
        self._head_fn = head_fn
        self._fetch_fn = fetch_fn

    async def head(self, url: str) -> int:
        return await asyncio.to_thread(self._head_fn, url)

    async def fetch_range(
        self,
        url: str,
        start: int,
        end: int,
        sink: Sink,
        chunk_size: int = STREAM_CHUNK,
    ) -> None:
        await asyncio.to_thread(self._fetch_fn, url, start, end, sink, chunk_size)


# ---------------------------------------------------------------------------
# Sync backend implementations (wrapped by ThreadedBackend)
# ---------------------------------------------------------------------------


def _requests_head(url: str) -> int:
    import requests

    resp = requests.head(url, allow_redirects=True)
    resp.raise_for_status()
    cl = resp.headers.get("content-length")
    if cl is None:
        raise RuntimeError("Server does not provide content-length.")
    return int(cl)


def _requests_fetch(
    url: str, start: int, end: int, sink: Sink, chunk_size: int = STREAM_CHUNK
) -> None:
    import requests

    with requests.get(
        url, headers={"Range": f"bytes={start}-{end}"}, stream=True
    ) as r:
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size):
            if chunk:
                sink(chunk)


def _pycurl_head(url: str) -> int:
    import pycurl

    headers: dict[str, str] = {}

    def header_cb(line: bytes) -> int:
        text = line.decode("iso-8859-1").strip()
        if ":" in text:
            k, v = text.split(":", 1)
            headers[k.strip().lower()] = v.strip()
        return len(line)

    c = pycurl.Curl()
    try:
        c.setopt(c.URL, url)
        c.setopt(c.NOBODY, 1)
        c.setopt(c.FOLLOWLOCATION, 1)
        c.setopt(c.HEADERFUNCTION, header_cb)
        c.perform()
    finally:
        c.close()

    cl = headers.get("content-length")
    if cl is None:
        raise RuntimeError("Server does not provide content-length.")
    return int(cl)


def _pycurl_fetch(
    url: str, start: int, end: int, sink: Sink, chunk_size: int = STREAM_CHUNK
) -> None:
    import pycurl

    def _write(data: bytes) -> int:
        sink(data)
        return len(data)

    c = pycurl.Curl()
    try:
        c.setopt(c.URL, url)
        c.setopt(c.HTTPHEADER, [f"Range: bytes={start}-{end}"])
        c.setopt(c.WRITEFUNCTION, _write)
        c.setopt(c.FOLLOWLOCATION, 1)
        c.perform()
        code = c.getinfo(c.RESPONSE_CODE)
        if code >= 400:
            raise RuntimeError(f"HTTP {code} for range {start}-{end}")
    finally:
        c.close()


def _pure_head(url: str) -> int:
    import urllib.request

    req = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(req) as resp:
        cl = resp.headers.get("Content-Length")
    if cl is None:
        raise RuntimeError("Server does not provide content-length.")
    return int(cl)


def _pure_fetch(
    url: str, start: int, end: int, sink: Sink, chunk_size: int = STREAM_CHUNK
) -> None:
    import urllib.request

    req = urllib.request.Request(url, headers={"Range": f"bytes={start}-{end}"})
    with urllib.request.urlopen(req) as resp:
        while True:
            chunk = resp.read(chunk_size)
            if not chunk:
                break
            sink(chunk)


def _httpx_head(url: str) -> int:
    import httpx

    with httpx.Client(follow_redirects=True) as client:
        r = client.head(url)
        r.raise_for_status()
        cl = r.headers.get("content-length")
    if cl is None:
        raise RuntimeError("Server does not provide content-length.")
    return int(cl)


def _httpx_fetch(
    url: str, start: int, end: int, sink: Sink, chunk_size: int = STREAM_CHUNK
) -> None:
    import httpx

    with httpx.Client(follow_redirects=True) as client:
        with client.stream(
            "GET", url, headers={"Range": f"bytes={start}-{end}"}
        ) as r:
            r.raise_for_status()
            for chunk in r.iter_bytes(chunk_size):
                if chunk:
                    sink(chunk)


# ---------------------------------------------------------------------------
# Async httpx backend (native)
# ---------------------------------------------------------------------------


class HttpxAsyncBackend:
    """Native ``asyncio`` backend built on ``httpx.AsyncClient``."""

    name = "httpx-async"

    async def head(self, url: str) -> int:
        import httpx

        async with httpx.AsyncClient(follow_redirects=True) as client:
            r = await client.head(url)
            r.raise_for_status()
            cl = r.headers.get("content-length")
        if cl is None:
            raise RuntimeError("Server does not provide content-length.")
        return int(cl)

    async def fetch_range(
        self,
        url: str,
        start: int,
        end: int,
        sink: Sink,
        chunk_size: int = STREAM_CHUNK,
    ) -> None:
        import httpx

        async with httpx.AsyncClient(follow_redirects=True) as client:
            async with client.stream(
                "GET", url, headers={"Range": f"bytes={start}-{end}"}
            ) as r:
                r.raise_for_status()
                async for chunk in r.aiter_bytes(chunk_size):
                    if chunk:
                        sink(chunk)


# ---------------------------------------------------------------------------
# Subprocess backends
# ---------------------------------------------------------------------------


async def _pump_subprocess(
    proc: asyncio.subprocess.Process,
    sink: Sink,
    chunk_size: int,
    tool: str,
) -> None:
    """Forward ``proc.stdout`` to *sink*, then raise if the process failed."""
    assert proc.stdout is not None
    try:
        while True:
            chunk = await proc.stdout.read(chunk_size)
            if not chunk:
                break
            sink(chunk)
    finally:
        err = b""
        if proc.stderr is not None:
            err = await proc.stderr.read()
        rc = await proc.wait()
        if rc != 0:
            raise RuntimeError(
                f"{tool} exited with code {rc}: "
                f"{err.decode(errors='replace').strip()}"
            )


def _parse_last_content_length(headers_text: str) -> int | None:
    """Return the last ``Content-Length`` header value found in *headers_text*."""
    value: str | None = None
    for line in headers_text.splitlines():
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        if k.strip().lower() == "content-length":
            value = v.strip()
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


class CurlBackend:
    """``curl`` driven through ``asyncio`` subprocesses."""

    name = "curl"

    async def head(self, url: str) -> int:
        proc = await asyncio.create_subprocess_exec(
            "curl",
            "-sIL",
            url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(
                f"curl failed: {err.decode(errors='replace').strip()}"
            )
        cl = _parse_last_content_length(out.decode("iso-8859-1"))
        if cl is None:
            raise RuntimeError("Server does not provide content-length.")
        return cl

    async def fetch_range(
        self,
        url: str,
        start: int,
        end: int,
        sink: Sink,
        chunk_size: int = STREAM_CHUNK,
    ) -> None:
        proc = await asyncio.create_subprocess_exec(
            "curl",
            "-sL",
            "-r",
            f"{start}-{end}",
            url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await _pump_subprocess(proc, sink, chunk_size, "curl")


class Aria2cBackend:
    """``aria2c`` driven through ``asyncio`` subprocesses.

    ``aria2c`` is asked for one range at a time and forced into single-connection
    mode (``--split=1``) because the orchestration layer already parallelises
    across ranges.  ``aria2c`` has no clean HEAD interface, so ``head`` falls
    back to :func:`_pure_head`.
    """

    name = "aria2c"

    async def head(self, url: str) -> int:
        return await asyncio.to_thread(_pure_head, url)

    async def fetch_range(
        self,
        url: str,
        start: int,
        end: int,
        sink: Sink,
        chunk_size: int = STREAM_CHUNK,
    ) -> None:
        proc = await asyncio.create_subprocess_exec(
            "aria2c",
            "--quiet=true",
            "--console-log-level=error",
            "--summary-interval=0",
            "--allow-overwrite=true",
            "--auto-file-renaming=false",
            "--split=1",
            "--max-connection-per-server=1",
            "--header",
            f"Range: bytes={start}-{end}",
            "--out=-",
            url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await _pump_subprocess(proc, sink, chunk_size, "aria2c")


# ---------------------------------------------------------------------------
# Backend registry
# ---------------------------------------------------------------------------

BACKENDS: dict[str, Callable[[], Backend]] = {
    "requests": lambda: ThreadedBackend("requests", _requests_head, _requests_fetch),
    "pycurl": lambda: ThreadedBackend("pycurl", _pycurl_head, _pycurl_fetch),
    "pure": lambda: ThreadedBackend("pure", _pure_head, _pure_fetch),
    "httpx": lambda: ThreadedBackend("httpx", _httpx_head, _httpx_fetch),
    "httpx-async": HttpxAsyncBackend,
    "curl": CurlBackend,
    "aria2c": Aria2cBackend,
}

_BACKEND_MODULE: dict[str, str] = {
    "requests": "requests",
    "pycurl": "pycurl",
    "httpx": "httpx",
    "httpx-async": "httpx",
}

_BACKEND_EXECUTABLE: dict[str, str] = {
    "curl": "curl",
    "aria2c": "aria2c",
}


def probe_backend(name: str) -> None:
    """Raise if *name* cannot run on this machine (missing import / executable)."""
    if (module := _BACKEND_MODULE.get(name)) is not None:
        __import__(module)
    elif (exe := _BACKEND_EXECUTABLE.get(name)) is not None:
        if shutil.which(exe) is None:
            raise FileNotFoundError(f"'{exe}' executable not found in PATH")


# ---------------------------------------------------------------------------
# Progress reporting
# ---------------------------------------------------------------------------


class ProgressCounter:
    """Thread-safe byte counter with a shared start timestamp."""

    def __init__(self, total: int) -> None:
        self.total = total
        self.start_time = time.monotonic()
        self._done = 0
        self._lock = threading.Lock()

    def add(self, n: int) -> None:
        with self._lock:
            self._done += n

    @property
    def done(self) -> int:
        with self._lock:
            return self._done


def _render_progress(done: int, total: int, start_time: float) -> None:
    elapsed = time.monotonic() - start_time
    speed = done / elapsed if elapsed > 0 else 0.0
    remaining = (total - done) / speed if speed > 0 else 0.0
    percent = (done / total * 100) if total > 0 else 0.0
    sys.stdout.write(
        f"\rProgress: {percent:6.2f}% | "
        f"Speed: {speed / 1024 / 1024:6.2f} MB/s | "
        f"ETA: {remaining:6.1f} sec"
    )
    sys.stdout.flush()


async def _progress_reporter(counter: ProgressCounter) -> None:
    """Redraw the progress line every :data:`PROGRESS_INTERVAL` seconds."""
    try:
        while True:
            _render_progress(counter.done, counter.total, counter.start_time)
            await asyncio.sleep(PROGRESS_INTERVAL)
    except asyncio.CancelledError:
        _render_progress(counter.done, counter.total, counter.start_time)
        raise


# ---------------------------------------------------------------------------
# Range planning & concurrent execution
# ---------------------------------------------------------------------------


def _plan_ranges(file_size: int, connections: int) -> list[Range]:
    """Split ``[0, file_size)`` into at most *connections* inclusive ranges."""
    connections = max(1, min(connections, file_size))
    chunk = file_size // connections
    ranges = [(i * chunk, (i + 1) * chunk - 1) for i in range(connections)]
    ranges[-1] = (ranges[-1][0], file_size - 1)
    return ranges


async def _gather_or_cancel(
    coros: Iterable[Coroutine[Any, Any, None]],
) -> None:
    """Run *coros* concurrently; cancel the rest if any of them raises."""
    tasks = [asyncio.create_task(c) for c in coros]
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


# ---------------------------------------------------------------------------
# Download strategies
# ---------------------------------------------------------------------------


async def _download_buffered(
    backend: Backend,
    url: str,
    ranges: Sequence[Range],
    counter: ProgressCounter,
) -> list[tuple[int, bytes]]:
    """Fetch all ranges into memory; caller assembles them in order."""
    results: list[tuple[int, bytes]] = []

    async def one(start: int, end: int) -> None:
        buf = BytesIO()

        def sink(chunk: bytes) -> None:
            buf.write(chunk)
            counter.add(len(chunk))

        await backend.fetch_range(url, start, end, sink)
        results.append((start, buf.getvalue()))

    await _gather_or_cancel(one(s, e) for s, e in ranges)
    return results


async def _download_chunked(
    backend: Backend,
    url: str,
    ranges: Sequence[Range],
    out_path: Path,
    counter: ProgressCounter,
) -> None:
    """Stream each range into a ``.part`` file, then concatenate them."""
    part_paths: dict[int, Path] = {
        start: out_path.with_name(f".{out_path.name}.part{start}")
        for start, _ in ranges
    }

    async def one(start: int, end: int) -> None:
        part_path = part_paths[start]
        written = 0
        with part_path.open("wb") as f:

            def sink(chunk: bytes) -> None:
                nonlocal written
                f.write(chunk)
                written += len(chunk)
                counter.add(len(chunk))

            await backend.fetch_range(url, start, end, sink)

        expected = end - start + 1
        if written != expected:
            raise RuntimeError(
                f"Range {start}-{end}: expected {expected} bytes, got {written}"
            )

    try:
        await _gather_or_cancel(one(s, e) for s, e in ranges)
        with out_path.open("wb") as out:
            for start, _ in sorted(part_paths):
                with part_paths[start].open("rb") as pf:
                    shutil.copyfileobj(pf, out, length=STREAM_CHUNK)
                part_paths[start].unlink()
    except BaseException:
        for p in part_paths.values():
            with contextlib.suppress(FileNotFoundError):
                p.unlink()
        raise


# ---------------------------------------------------------------------------
# Top-level download
# ---------------------------------------------------------------------------


async def download_url(
    url: str,
    connections: int,
    backend: Backend,
    out_dir: Path,
) -> Path:
    """Download *url* into *out_dir*, returning the resulting file path."""
    filename = Path(urlparse(url).path).name or "downloaded_file"
    out_path = out_dir / filename

    file_size = await backend.head(url)
    if file_size <= 0:
        raise RuntimeError(f"Refusing to download empty resource: {url}")

    ranges = _plan_ranges(file_size, connections)
    effective_connections = len(ranges)

    use_chunked = file_size > CHUNK_THRESHOLD
    mode = "chunked→disk" if use_chunked else "buffered"
    print(
        f"File: {filename} | Size: {file_size / 1024 / 1024:.2f} MB | "
        f"Connections: {effective_connections} | Backend: {backend.name} | "
        f"Mode: {mode}"
    )

    counter = ProgressCounter(file_size)
    reporter = asyncio.create_task(_progress_reporter(counter))

    try:
        if use_chunked:
            await _download_chunked(backend, url, ranges, out_path, counter)
        else:
            results = await _download_buffered(backend, url, ranges, counter)
            results.sort(key=lambda item: item[0])
            with out_path.open("wb") as f:
                for _, data in results:
                    f.write(data)
    finally:
        reporter.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await reporter

    print(f"\n✅ Download complete: {out_path}")
    return out_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def read_urls_from_file(path: Path) -> list[str]:
    """Return non-empty, non-comment lines from *path*."""
    urls: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        urls.append(line)
    return urls


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="wget2",
        description="Multi-connection downloader with pluggable HTTP backends.",
    )
    p.add_argument("url", nargs="?", help="URL to download (omit if using -f).")
    p.add_argument(
        "-c",
        "--connections",
        type=int,
        default=8,
        metavar="N",
        help="parallel connections per URL (default: 8).",
    )
    p.add_argument(
        "-b",
        "--backend",
        choices=list(BACKENDS),
        default="httpx-async",
        help="HTTP backend to use (default: httpx-async).",
    )
    p.add_argument(
        "-f",
        "--file",
        type=Path,
        metavar="FILE",
        help="read URLs from FILE (one per line, '#' comments allowed).",
    )
    p.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=Path("."),
        help="directory to write downloads into (default: current dir).",
    )
    return p


async def _run(
    urls: Sequence[str],
    connections: int,
    backend: Backend,
    out_dir: Path,
) -> None:
    for url in urls:
        try:
            await download_url(url, connections, backend, out_dir)
        except Exception as exc:  # noqa: BLE001 - report and continue
            print(f"error downloading {url}: {exc}", file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    urls: list[str] = []
    if args.file:
        if not args.file.is_file():
            print(f"error: URL file not found: {args.file}", file=sys.stderr)
            return 2
        urls.extend(read_urls_from_file(args.file))
    if args.url:
        urls.append(args.url)
    if not urls:
        print("error: provide a URL or use -f FILE", file=sys.stderr)
        return 2

    try:
        probe_backend(args.backend)
    except (ImportError, FileNotFoundError) as exc:
        print(
            f"error: backend '{args.backend}' unavailable: {exc}",
            file=sys.stderr,
        )
        return 1

    out_dir: Path = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    backend = BACKENDS[args.backend]()

    try:
        asyncio.run(_run(urls, args.connections, backend, out_dir))
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())