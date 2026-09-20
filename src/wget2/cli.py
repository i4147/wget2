from __future__ import annotations

import argparse
import asyncio
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from io import BytesIO
from multiprocessing import Pool, Value
from pathlib import Path
from urllib.parse import urlparse

from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CHUNK_THRESHOLD = 10 * 1024 * 1024   # >10 MiB → stream parts to disk
STREAM_CHUNK = 1024 * 1024           # 1 MiB per read
POLL_INTERVAL = 0.1                  # seconds between progress polls

Sink = Callable[[bytes], object]
HeadFn = Callable[[str], int]
FetchRangeFn = Callable[[str, int, int, Sink], None]
Backend = tuple[HeadFn, FetchRangeFn]
BackendFactory = Callable[[], Backend]


# ---------------------------------------------------------------------------
# Backends
# ---------------------------------------------------------------------------

def make_requests_backend() -> Backend:
    """Synchronous backend built on :mod:`requests`."""
    import requests

    def head(url: str) -> int:
        resp = requests.head(url, allow_redirects=True)
        resp.raise_for_status()
        cl = resp.headers.get("content-length")
        if cl is None:
            raise RuntimeError("Server does not provide content-length.")
        return int(cl)

    def fetch_range(url: str, start: int, end: int, sink: Sink) -> None:
        with requests.get(
            url,
            headers={"Range": f"bytes={start}-{end}"},
            stream=True,
        ) as r:
            r.raise_for_status()
            for chunk in r.iter_content(STREAM_CHUNK):
                if chunk:
                    sink(chunk)

    return head, fetch_range


def make_pycurl_backend() -> Backend:
    """Synchronous backend built on :mod:`pycurl`."""
    import pycurl

    def head(url: str) -> int:
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

    def fetch_range(url: str, start: int, end: int, sink: Sink) -> None:
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

    return head, fetch_range


def make_pure_backend() -> Backend:
    """Pure-stdlib backend using :mod:`urllib.request`."""
    import urllib.request

    def head(url: str) -> int:
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req) as resp:
            cl = resp.headers.get("Content-Length")
        if cl is None:
            raise RuntimeError("Server does not provide content-length.")
        return int(cl)

    def fetch_range(url: str, start: int, end: int, sink: Sink) -> None:
        req = urllib.request.Request(
            url, headers={"Range": f"bytes={start}-{end}"}
        )
        with urllib.request.urlopen(req) as resp:
            while True:
                chunk = resp.read(STREAM_CHUNK)
                if not chunk:
                    break
                sink(chunk)

    return head, fetch_range


def make_httpx_backend() -> Backend:
    """Synchronous backend built on :mod:`httpx`."""
    import httpx

    def head(url: str) -> int:
        with httpx.Client(follow_redirects=True) as client:
            resp = client.head(url)
            resp.raise_for_status()
            cl = resp.headers.get("content-length")
            if cl is None:
                raise RuntimeError("Server does not provide content-length.")
            return int(cl)

    def fetch_range(url: str, start: int, end: int, sink: Sink) -> None:
        with httpx.Client(follow_redirects=True) as client:
            with client.stream(
                "GET", url, headers={"Range": f"bytes={start}-{end}"}
            ) as r:
                r.raise_for_status()
                for chunk in r.iter_bytes(STREAM_CHUNK):
                    if chunk:
                        sink(chunk)

    return head, fetch_range


def make_httpx_async_backend() -> Backend:
    """Async :mod:`httpx` backend.

    Each call drives its own event loop via :func:`asyncio.run`, which keeps
    the overall architecture identical to the synchronous backends (one
    process / one range) while still using ``httpx.AsyncClient`` under the
    hood.
    """
    import httpx

    async def _head_async(url: str) -> int:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.head(url)
            resp.raise_for_status()
            cl = resp.headers.get("content-length")
            if cl is None:
                raise RuntimeError("Server does not provide content-length.")
            return int(cl)

    async def _fetch_async(
        url: str, start: int, end: int, sink: Sink
    ) -> None:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            async with client.stream(
                "GET", url, headers={"Range": f"bytes={start}-{end}"}
            ) as r:
                r.raise_for_status()
                async for chunk in r.aiter_bytes(STREAM_CHUNK):
                    if chunk:
                        sink(chunk)

    def head(url: str) -> int:
        return asyncio.run(_head_async(url))

    def fetch_range(url: str, start: int, end: int, sink: Sink) -> None:
        asyncio.run(_fetch_async(url, start, end, sink))

    return head, fetch_range


def _http_head_via_urllib(url: str) -> int:
    """HEAD via urllib; used as a helper by subprocess-based backends."""
    import urllib.request

    req = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(req) as resp:
        cl = resp.headers.get("Content-Length")
    if cl is None:
        raise RuntimeError("Server does not provide content-length.")
    return int(cl)


def _stream_pipe(proc: subprocess.Popen, sink: Sink, tool: str) -> None:
    """Forward ``proc.stdout`` to ``sink`` and raise on non-zero exit."""
    assert proc.stdout is not None
    try:
        while True:
            chunk = proc.stdout.read(STREAM_CHUNK)
            if not chunk:
                break
            sink(chunk)
    finally:
        proc.wait()
    if proc.returncode != 0:
        err = (
            proc.stderr.read().decode("utf-8", "replace")
            if proc.stderr
            else ""
        )
        raise RuntimeError(f"{tool} exited with {proc.returncode}: {err.strip()}")


def make_curl_backend() -> Backend:
    """Backend that shells out to the ``curl`` binary for each range."""

    def head(url: str) -> int:
        return _http_head_via_urllib(url)

    def fetch_range(url: str, start: int, end: int, sink: Sink) -> None:
        proc = subprocess.Popen(
            ["curl", "-sL", "-r", f"{start}-{end}", url],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        _stream_pipe(proc, sink, "curl")

    return head, fetch_range


def make_aria2c_backend() -> Backend:
    """Backend that shells out to the ``aria2c`` binary for each range.

    ``aria2c`` does not stream directly to a pipe cleanly across all
    versions, so each worker downloads its range to a temporary file and
    then replays it through ``sink``.
    """

    def head(url: str) -> int:
        return _http_head_via_urllib(url)

    def fetch_range(url: str, start: int, end: int, sink: Sink) -> None:
        with tempfile.TemporaryDirectory(prefix="wget2-aria2c-") as tmp:
            out_file = Path(tmp) / "part"
            proc = subprocess.run(
                [
                    "aria2c",
                    "--quiet=true",
                    "--allow-overwrite=true",
                    "--max-connection-per-server=1",
                    "--split=1",
                    "--file-allocation=none",
                    "--max-tries=1",
                    "--header",
                    f"Range: bytes={start}-{end}",
                    "--dir",
                    tmp,
                    "--out",
                    out_file.name,
                    url,
                ],
                capture_output=True,
            )
            if proc.returncode != 0:
                err = proc.stderr.decode("utf-8", "replace")
                raise RuntimeError(
                    f"aria2c exited with {proc.returncode}: {err.strip()}"
                )
            with out_file.open("rb") as f:
                while True:
                    chunk = f.read(STREAM_CHUNK)
                    if not chunk:
                        break
                    sink(chunk)

    return head, fetch_range


BACKENDS: dict[str, BackendFactory] = {
    "requests": make_requests_backend,
    "pycurl": make_pycurl_backend,
    "pure": make_pure_backend,
    "httpx": make_httpx_backend,
    "httpx_async": make_httpx_async_backend,
    "aria2c": make_aria2c_backend,
    "curl": make_curl_backend,
}

DEFAULT_BACKEND = "httpx_async"


# ---------------------------------------------------------------------------
# Multiprocessing workers (must be top-level to be picklable)
# ---------------------------------------------------------------------------

def _range_worker_memory(task: tuple[str, str, int, int]) -> tuple[int, bytes]:
    """Download a range into memory (used for small files)."""
    backend_name, url, start, end = task
    _, fetch_range = BACKENDS[backend_name]()
    buf = BytesIO()
    fetch_range(url, start, end, buf.write)
    return start, buf.getvalue()


def _range_worker_chunked(
    task: tuple[str, str, int, int, str, "Value"],
) -> tuple[int, str, int]:
    """Download a range to its own part-file (used for large files)."""
    backend_name, url, start, end, part_path_str, counter = task
    _, fetch_range = BACKENDS[backend_name]()
    part_path = Path(part_path_str)

    written = 0
    with part_path.open("wb") as f:

        def sink(chunk: bytes) -> None:
            nonlocal written
            f.write(chunk)
            written += len(chunk)
            with counter.get_lock():
                counter.value += len(chunk)

        fetch_range(url, start, end, sink)

    expected = end - start + 1
    if written != expected:
        raise RuntimeError(
            f"Range {start}-{end}: expected {expected} bytes, got {written}"
        )
    return start, str(part_path), written


# ---------------------------------------------------------------------------
# Progress bar
# ---------------------------------------------------------------------------

def _make_progress() -> Progress:
    """Build a rich progress bar with bytes / speed / elapsed / ETA."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=None),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        transient=False,
    )


# ---------------------------------------------------------------------------
# Range planning
# ---------------------------------------------------------------------------

def _plan_ranges(file_size: int, connections: int) -> list[tuple[int, int]]:
    """Split ``file_size`` into ``connections`` inclusive ranges."""
    connections = max(1, min(connections, file_size))
    chunk = file_size // connections
    ranges = [(i * chunk, (i + 1) * chunk - 1) for i in range(connections)]
    ranges[-1] = (ranges[-1][0], file_size - 1)
    return ranges


# ---------------------------------------------------------------------------
# Download strategies
# ---------------------------------------------------------------------------

def _download_buffered(
    url: str,
    ranges: list[tuple[int, int]],
    backend_name: str,
    file_size: int,
    num_connections: int,
) -> list[tuple[int, bytes]]:
    """Download each range into memory and return them sorted by start."""
    results: list[tuple[int, bytes]] = []

    with _make_progress() as progress:
        task_id = progress.add_task("Downloading", total=file_size)

        with Pool(processes=num_connections) as pool:
            pending = [
                (
                    pool.apply_async(
                        _range_worker_memory, ((backend_name, url, s, e),)
                    ),
                    s,
                )
                for s, e in ranges
            ]

            while pending:
                for item in list(pending):
                    ar, _ = item
                    if not ar.ready():
                        continue
                    _, data = ar.get()
                    results.append((_, data))
                    progress.update(task_id, advance=len(data))
                    pending.remove(item)
                if pending:
                    time.sleep(POLL_INTERVAL / 4)

    return results


def _download_chunked(
    url: str,
    ranges: list[tuple[int, int]],
    backend_name: str,
    file_size: int,
    num_connections: int,
    out_path: Path,
) -> None:
    """Download each range to a part-file, then concatenate them."""
    part_paths: dict[int, Path] = {
        s: out_path.with_name(f".{out_path.name}.part{s}") for s, _ in ranges
    }

    counter = Value("q", 0)

    try:
        with _make_progress() as progress:
            task_id = progress.add_task("Downloading", total=file_size)
            last_seen = 0

            with Pool(processes=num_connections) as pool:
                pending = [
                    (
                        pool.apply_async(
                            _range_worker_chunked,
                            (
                                (
                                    backend_name,
                                    url,
                                    s,
                                    e,
                                    str(part_paths[s]),
                                    counter,
                                ),
                            ),
                        ),
                        s,
                    )
                    for s, e in ranges
                ]

                while pending:
                    for item in list(pending):
                        ar, _ = item
                        if ar.ready():
                            ar.get()
                            pending.remove(item)

                    current = counter.value
                    if current > last_seen:
                        progress.update(task_id, advance=current - last_seen)
                        last_seen = current
                    if pending:
                        time.sleep(POLL_INTERVAL)

                # Final flush after all workers finish.
                current = counter.value
                if current > last_seen:
                    progress.update(task_id, advance=current - last_seen)

        # Concatenate the part files into the final destination.
        with out_path.open("wb") as out:
            for s, _ in sorted(part_paths):
                with part_paths[s].open("rb") as pf:
                    shutil.copyfileobj(pf, out, length=STREAM_CHUNK)
                part_paths[s].unlink()
    except BaseException:
        for p in part_paths.values():
            try:
                p.unlink()
            except FileNotFoundError:
                pass
        raise


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def download_url(
    url: str,
    connections: int,
    backend_name: str,
    out_dir: Path,
) -> Path:
    """Download a single URL using the chosen backend."""
    head, _ = BACKENDS[backend_name]()

    filename = Path(urlparse(url).path).name or "downloaded_file"
    out_path = out_dir / filename

    file_size = head(url)
    if file_size <= 0:
        raise RuntimeError(f"Refusing to download empty resource: {url}")

    ranges = _plan_ranges(file_size, connections)
    effective_connections = len(ranges)

    use_chunked = file_size > CHUNK_THRESHOLD
    mode = "chunked→disk" if use_chunked else "buffered"

    console = Console()
    console.print(
        f"[bold]File:[/bold] {filename}  "
        f"[bold]Size:[/bold] {file_size / 1024 / 1024:.2f} MB  "
        f"[bold]Connections:[/bold] {effective_connections}  "
        f"[bold]Backend:[/bold] {backend_name}  "
        f"[bold]Mode:[/bold] {mode}"
    )

    if use_chunked:
        _download_chunked(
            url, ranges, backend_name, file_size, effective_connections, out_path
        )
    else:
        results = _download_buffered(
            url, ranges, backend_name, file_size, effective_connections
        )
        results.sort(key=lambda x: x[0])
        with out_path.open("wb") as f:
            for _, data in results:
                f.write(data)

    console.print(f"[bold green]✅ Download complete:[/bold green] {out_path}")
    return out_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def read_urls_from_file(path: Path) -> list[str]:
    """Read URLs from ``path``, one per line; ``#`` starts a comment."""
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
        default=DEFAULT_BACKEND,
        help=f"HTTP backend to use (default: {DEFAULT_BACKEND}).",
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

    out_dir: Path = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    # Validate backend availability early (missing optional dependency).
    try:
        BACKENDS[args.backend]()
    except ImportError as exc:
        print(
            f"error: backend '{args.backend}' unavailable: {exc}",
            file=sys.stderr,
        )
        return 1

    for url in urls:
        try:
            download_url(url, args.connections, args.backend, out_dir)
        except Exception as exc:
            print(f"error downloading {url}: {exc}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())