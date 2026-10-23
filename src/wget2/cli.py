from __future__ import annotations

import asyncio
import sys
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from io import BytesIO
from multiprocessing import Pool, Value
from pathlib import Path
from typing import TypeAlias
from urllib.parse import urlparse

import typer
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

CHUNK_THRESHOLD = 10 * 1024 * 1024
STREAM_CHUNK = 1024 * 1024
POLL_INTERVAL = 0.1

Sink: TypeAlias = Callable[[bytes], object]
ConsoleType: TypeAlias = Console

class DownloadBackend(ABC):
    @abstractmethod
    def head(self, url: str) -> int:
        ...

    @abstractmethod
    def fetch_range(self, url: str, start: int, end: int, sink: Sink) -> None:
        ...

class RequestsBackend(DownloadBackend):
    def head(self, url: str) -> int:
        import requests

        resp = requests.head(url, allow_redirects=True)
        resp.raise_for_status()
        cl = resp.headers.get("content-length")
        if cl is None:
            raise RuntimeError("Server does not provide content-length.")
        return int(cl)

    def fetch_range(self, url: str, start: int, end: int, sink: Sink) -> None:
        import requests

        with requests.get(
            url,
            headers={"Range": f"bytes={start}-{end}"},
            stream=True,
        ) as r:
            r.raise_for_status()
            for chunk in r.iter_content(STREAM_CHUNK):
                if chunk:
                    sink(chunk)

class PycurlBackend(DownloadBackend):
    def head(self, url: str) -> int:
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

    def fetch_range(self, url: str, start: int, end: int, sink: Sink) -> None:
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

class HttpxBackend(DownloadBackend):
    def head(self, url: str) -> int:
        import httpx

        with httpx.Client(follow_redirects=True) as client:
            resp = client.head(url)
            resp.raise_for_status()
            cl = resp.headers.get("content-length")
            if cl is None:
                raise RuntimeError("Server does not provide content-length.")
            return int(cl)

    def fetch_range(self, url: str, start: int, end: int, sink: Sink) -> None:
        import httpx

        with httpx.Client(follow_redirects=True) as client:
            with client.stream("GET", url, headers={"Range": f"bytes={start}-{end}"}) as r:
                r.raise_for_status()
                for chunk in r.iter_bytes(STREAM_CHUNK):
                    if chunk:
                        sink(chunk)

class HttpxAsyncBackend(DownloadBackend):
    def head(self, url: str) -> int:
        return asyncio.run(self._head_async(url))

    def fetch_range(self, url: str, start: int, end: int, sink: Sink) -> None:
        asyncio.run(self._fetch_async(url, start, end, sink))

    async def _head_async(self, url: str) -> int:
        import httpx

        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.head(url)
            resp.raise_for_status()
            cl = resp.headers.get("content-length")
            if cl is None:
                raise RuntimeError("Server does not provide content-length.")
            return int(cl)

    async def _fetch_async(self, url: str, start: int, end: int, sink: Sink) -> None:
        import httpx

        async with httpx.AsyncClient(follow_redirects=True) as client:
            async with client.stream("GET", url, headers={"Range": f"bytes={start}-{end}"}) as r:
                r.raise_for_status()
                async for chunk in r.aiter_bytes(STREAM_CHUNK):
                    if chunk:
                        sink(chunk)

BACKENDS: dict[str, type[DownloadBackend]] = {
    "requests": RequestsBackend,
    "pycurl": PycurlBackend,
    "httpx": HttpxBackend,
    "httpx_async": HttpxAsyncBackend,
}

DEFAULT_BACKEND = "httpx_async"

def _range_worker_memory(task: tuple[str, str, int, int]) -> tuple[int, bytes]:
    backend_name, url, start, end = task
    backend = BACKENDS[backend_name]()
    buf = BytesIO()
    backend.fetch_range(url, start, end, buf.write)
    return start, buf.getvalue()

def _range_worker_chunked(
    task: tuple[str, str, int, int, str, Value],
) -> tuple[int, str, int]:
    backend_name, url, start, end, part_path_str, counter = task
    backend = BACKENDS[backend_name]()
    part_path = Path(part_path_str)
    written = 0
    with part_path.open("wb") as f:

        def sink(chunk: bytes) -> None:
            nonlocal written
            f.write(chunk)
            written += len(chunk)
            with counter.get_lock():
                counter.value += len(chunk)

        backend.fetch_range(url, start, end, sink)
    expected = end - start + 1
    if written != expected:
        raise RuntimeError(f"Range {start}-{end}: expected {expected} bytes, got {written}")
    return start, str(part_path), written

def _make_progress() -> Progress:
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

def _plan_ranges(file_size: int, connections: int) -> list[tuple[int, int]]:
    connections = max(1, min(connections, file_size))
    chunk = file_size // connections
    ranges = [(i * chunk, (i + 1) * chunk - 1) for i in range(connections)]
    ranges[-1] = (ranges[-1][0], file_size - 1)
    return ranges

def _download_buffered(
    url: str,
    ranges: list[tuple[int, int]],
    backend_name: str,
    file_size: int,
    num_connections: int,
) -> list[tuple[int, bytes]]:
    results: list[tuple[int, bytes]] = []
    with _make_progress() as progress:
        task_id = progress.add_task("Downloading", total=file_size)
        with Pool(processes=num_connections) as pool:
            pending = [
                (
                    pool.apply_async(_range_worker_memory, ((backend_name, url, s, e),)),
                    s,
                )
                for s, e in ranges
            ]
            while pending:
                for item in list(pending):
                    ar, _ = item
                    if not ar.ready():
                        continue
                    start, data = ar.get()
                    results.append((start, data))
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
    import shutil

    part_paths = {s: out_path.with_name(f".{out_path.name}.part{s}") for s, _ in ranges}
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
                current = counter.value
                if current > last_seen:
                    progress.update(task_id, advance=current - last_seen)
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

def download_url(
    url: str,
    connections: int,
    backend_name: str,
    out_dir: Path,
    console: ConsoleType,
) -> Path:
    backend = BACKENDS[backend_name]()
    filename = Path(urlparse(url).path).name or "downloaded_file"
    out_path = out_dir / filename
    file_size = backend.head(url)
    if file_size <= 0:
        raise RuntimeError(f"Refusing to download empty resource: {url}")
    ranges = _plan_ranges(file_size, connections)
    effective_connections = len(ranges)
    use_chunked = file_size > CHUNK_THRESHOLD
    mode = "chunked→disk" if use_chunked else "buffered"
    console.print(
        f"[bold]File:[/bold] {filename}  "
        f"[bold]Size:[/bold] {file_size / 1024 / 1024:.2f} MB  "
        f"[bold]Connections:[/bold] {effective_connections}  "
        f"[bold]Backend:[/bold] {backend_name}  "
        f"[bold]Mode:[/bold] {mode}"
    )
    if use_chunked:
        _download_chunked(url, ranges, backend_name, file_size, effective_connections, out_path)
    else:
        results = _download_buffered(url, ranges, backend_name, file_size, effective_connections)
        results.sort(key=lambda x: x[0])
        with out_path.open("wb") as f:
            for _, data in results:
                f.write(data)
    console.print(f"[bold green]✅ Download complete:[/bold green] {out_path}")
    return out_path

def read_urls_from_file(path: Path) -> list[str]:
    urls: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        urls.append(line)
    return urls

app = typer.Typer(
    name="wget2",
    help="Multi-connection downloader with pluggable HTTP backends.",
    add_completion=False,
    rich_markup_mode="rich",
)

@app.command()
def main(
    url: str | None = typer.Argument(None, help="URL to download (omit if using -f)."),
    connections: int = typer.Option(
        8,
        "--connections",
        "-c",
        metavar="N",
        help="Parallel connections per URL.",
    ),
    backend: str = typer.Option(
        DEFAULT_BACKEND,
        "--backend",
        "-b",
        help=f"HTTP backend to use. Choices: {', '.join(BACKENDS)}",
    ),
    file: Path | None = typer.Option(
        None,
        "--file",
        "-f",
        metavar="FILE",
        help="Read URLs from FILE (one per line, '#' comments allowed).",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
    output_dir: Path = typer.Option(
        Path("."),
        "--output-dir",
        "-o",
        help="Directory to write downloads into.",
        file_okay=False,
        dir_okay=True,
        writable=True,
    ),
) -> None:
    console = Console()
    if backend not in BACKENDS:
        console.print(
            f"[bold red]error:[/bold red] unknown backend '{backend}'. "
            f"Available: {', '.join(BACKENDS)}"
        )
        raise typer.Exit(code=1)
    urls: list[str] = []
    if file is not None:
        urls.extend(read_urls_from_file(file))
    if url:
        urls.append(url)
    if not urls:
        console.print("[bold red]error:[/bold red] provide a URL or use -f FILE")
        raise typer.Exit(code=2)
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        BACKENDS[backend]()
    except ImportError as exc:
        console.print(f"[bold red]error:[/bold red] backend '{backend}' unavailable: {exc}")
        raise typer.Exit(code=1)
    for u in urls:
        try:
            download_url(u, connections, backend, output_dir, console)
        except Exception as exc:
            console.print(f"[bold red]error downloading {u}:[/bold red] {exc}")
            raise typer.Exit(code=1)

if __name__ == "__main__":
    app()