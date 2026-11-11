"""Core single-file download engine: probe, resume, ranges, retries, finalize."""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Callable

from .backends.base import DownloadBackend, ProbeResult, ResponseInfo, parse_content_range
from .backends.registry import BackendRegistry
from .checksum import ChecksumSpec, specs_for, verify_file
from .errors import (
    EXIT_HTTP,
    EXIT_INTERRUPTED,
    EXIT_NETWORK,
    EXIT_OK,
    ContentLengthError,
    FilesystemError,
    IntegrityError,
    Interrupted,
    NetworkError,
    PolicyError,
    RangeNotSupportedError,
    UsageError,
    Wget2Error,
    exit_code_for,
)
from .log import get_logger
from .net import BackendPool, HostLimiter, PathLocks
from .output import resolve_output_path, write_manifest
from .progress import ProgressReporter, TaskHandle
from .ratelimit import MultiLimiter, RateLimiter
from .request import RequestConfig
from .settings import SEGMENTS_PER_CONNECTION, Settings
from .state import Interval, PartState, ensure_directory, open_part_state, plan_ranges
from .util import (
    apply_mtime,
    atomic_finalize,
    ensure_free_space,
    human_size,
    preallocate,
    redact_url,
    safe_unlink,
    validate_url,
)

PART_SUFFIX = ".part"
#: fsync + checkpoint the resume state at least this often
SYNC_INTERVAL = 8 * 1024 * 1024
MULTI_MIN_SIZE = 1024 * 1024


@dataclass
class FileResult:
    url: str
    index: int = 0
    status: str = "failed"  # downloaded | skipped | up-to-date | spidered | dry-run | failed
    path: Path | None = None
    size: int | None = None
    bytes_written: int = 0
    duration: float = 0.0
    backend: str | None = None
    attempts: int = 1
    connections: int = 0
    resumed: bool = False
    checksums: dict[str, str] = field(default_factory=dict)
    etag: str | None = None
    last_modified: str | None = None
    error: str | None = None
    error_kind: str | None = None
    exit_code: int = EXIT_OK
    detail: str | None = None

    @property
    def ok(self) -> bool:
        return self.exit_code == EXIT_OK

    @property
    def skipped(self) -> bool:
        return self.status in {"skipped", "up-to-date"}

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "url": redact_url(self.url),
            "status": self.status,
            "path": str(self.path) if self.path else None,
            "size": self.size,
            "bytes_written": self.bytes_written,
            "duration": round(self.duration, 3),
            "backend": self.backend,
            "connections": self.connections,
            "resumed": self.resumed,
            "attempts": self.attempts,
            "exit_code": self.exit_code,
        }
        if self.checksums:
            payload["checksums"] = self.checksums
        if self.etag:
            payload["etag"] = self.etag
        if self.last_modified:
            payload["last_modified"] = self.last_modified
        if self.error:
            payload["error"] = self.error
        if self.error_kind:
            payload["error_kind"] = self.error_kind
        if self.detail:
            payload["detail"] = self.detail
        return payload


class InterruptFlag:
    """Cooperative SIGINT flag shared with worker threads."""

    def __init__(self) -> None:
        self._event = threading.Event()

    def set(self) -> None:
        self._event.set()

    def is_set(self) -> bool:
        return self._event.is_set()

    def clear(self) -> None:
        self._event.clear()

    def check(self) -> None:
        if self._event.is_set():
            raise Interrupted("interrupted by user")


@dataclass
class DownloadContext:
    settings: Settings
    registry: BackendRegistry
    pool: BackendPool
    progress: ProgressReporter
    host_limiter: HostLimiter
    interrupt: InterruptFlag
    path_locks: PathLocks = field(default_factory=PathLocks)
    global_limiter: MultiLimiter = field(default_factory=lambda: MultiLimiter([]))
    log: Any = field(default_factory=lambda: get_logger("download"))

    @classmethod
    def build(
        cls,
        settings: Settings,
        registry: BackendRegistry,
        progress: ProgressReporter,
        *,
        request_config: RequestConfig | None = None,
    ) -> DownloadContext:
        config = request_config or settings.request
        pool = BackendPool(
            registry,
            config,
            preferred=settings.backend,
            extra=settings.backend_extra,
            fallback=settings.backend_auto_fallback or settings.backend == "auto",
        )
        global_limiter = settings.global_limiter()
        return cls(
            settings=settings,
            registry=registry,
            pool=pool,
            progress=progress,
            host_limiter=HostLimiter(settings.max_per_host),
            interrupt=InterruptFlag(),
            global_limiter=MultiLimiter([global_limiter]),
        )

    def close(self) -> None:
        self.pool.close()


# --------------------------------------------------------------------------- #
# streaming sinks
# --------------------------------------------------------------------------- #
class DiskSink:
    """Write chunks at an absolute offset in a pre-allocated file."""

    def __init__(
        self,
        fd: int,
        offset: int,
        *,
        state: PartState,
        progress: ProgressReporter,
        handle: TaskHandle,
        limiters: MultiLimiter,
        interrupt: InterruptFlag,
    ) -> None:
        self.fd = fd
        self.offset = offset
        self.start = offset
        self.written = 0
        self._state = state
        self._progress = progress
        self._handle = handle
        self._limiters = limiters
        self._interrupt = interrupt
        self._unsynced = 0

    def feed(self, chunk: bytes) -> None:
        self._interrupt.check()
        if not chunk:
            return
        if self._limiters.enabled:
            self._limiters.consume(len(chunk))
        view = memoryview(chunk)
        sent = 0
        try:
            while sent < len(view):
                sent += os.pwrite(self.fd, view[sent:], self.offset + sent)
        except OSError as exc:
            raise FilesystemError(f"write failed at offset {self.offset}: {exc}") from exc
        self.offset += len(view)
        self.written += len(view)
        self._state.mark(self.start, self.offset)
        self._progress.update(self._handle, advance=len(view))
        self._unsynced += len(view)
        if self._unsynced >= SYNC_INTERVAL:
            self.checkpoint()

    def checkpoint(self) -> None:
        try:
            os.fsync(self.fd)
        except OSError:  # pragma: no cover - non fatal
            pass
        self._state.save()
        self._unsynced = 0

    __call__ = feed


class MemorySink:
    """Buffer a segment in RAM (files below ``--buffer-threshold``)."""

    def __init__(
        self,
        *,
        progress: ProgressReporter,
        handle: TaskHandle,
        limiters: MultiLimiter,
        interrupt: InterruptFlag,
    ) -> None:
        self.buffer = bytearray()
        self.written = 0
        self._progress = progress
        self._handle = handle
        self._limiters = limiters
        self._interrupt = interrupt

    def feed(self, chunk: bytes) -> None:
        self._interrupt.check()
        if not chunk:
            return
        if self._limiters.enabled:
            self._limiters.consume(len(chunk))
        self.buffer.extend(chunk)
        self.written += len(chunk)
        self._progress.update(self._handle, advance=len(chunk))

    __call__ = feed

    def reset(self) -> None:
        self.buffer.clear()
        self.written = 0


# --------------------------------------------------------------------------- #
# per-file downloader
# --------------------------------------------------------------------------- #
class FileDownloader:
    def __init__(self, context: DownloadContext) -> None:
        self.ctx = context
        self.settings = context.settings
        self.log = get_logger("download")

    # -- entry point ------------------------------------------------------ #
    def run(self, url: str, index: int = 0) -> FileResult:
        """Download ``url``, falling back to ``--mirror-list`` URLs on failure."""
        started = time.monotonic()
        mirrors = [m for m in self.settings.mirrors if m and m != url]
        candidates = [url, *mirrors]
        result = FileResult(url=url, index=index)
        for attempt, candidate in enumerate(candidates, start=1):
            result = FileResult(url=url, index=index)
            self._attempt(candidate, url, index, result, started)
            result.attempts = attempt
            if result.ok or result.exit_code not in {EXIT_NETWORK, EXIT_HTTP}:
                break
            if attempt < len(candidates):
                self.log.warning(
                    "mirror failed, trying next",
                    url=redact_url(candidate),
                    next=redact_url(candidates[attempt]),
                    error=result.error,
                )
        result.duration = time.monotonic() - started
        return result

    def _attempt(self, candidate: str, name_url: str, index: int, result: FileResult, started: float) -> None:
        """One download attempt against ``candidate`` (named after ``name_url``)."""
        try:
            candidate = validate_url(candidate, allow_extra_schemes=self.settings.allow_unsafe_schemes)
            probe, backend_name = self._probe_with_fallback(candidate)
            result.backend = backend_name
            result.etag = probe.etag
            result.last_modified = probe.last_modified
            result.size = probe.size
            self._execute(candidate, index, probe, backend_name, result, started, name_url)
        except Interrupted as exc:
            result.status = "failed"
            result.error = str(exc)
            result.error_kind = exc.kind
            result.exit_code = EXIT_INTERRUPTED
        except Wget2Error as exc:
            result.status = "failed"
            if self.ctx.interrupt.is_set():
                # the interrupt aborted the transfer; report it as such
                result.error = "interrupted by user"
                result.error_kind = "interrupted"
                result.exit_code = EXIT_INTERRUPTED
                self.log.info("download interrupted", url=redact_url(result.url))
            else:
                result.error = str(exc)
                result.error_kind = exc.kind
                result.exit_code = exc.exit_code
                self.log.error(
                    "download failed",
                    url=redact_url(candidate),
                    error=str(exc),
                    kind=exc.kind,
                )
        except KeyboardInterrupt:  # pragma: no cover - main thread only
            result.status = "failed"
            result.error = "interrupted"
            result.error_kind = "interrupted"
            result.exit_code = EXIT_INTERRUPTED
        except OSError as exc:
            result.status = "failed"
            result.error = f"{exc.__class__.__name__}: {exc}"
            result.error_kind = "filesystem"
            result.exit_code = exit_code_for(exc)
        except Exception as exc:  # pragma: no cover - unexpected
            result.status = "failed"
            result.error = f"{exc.__class__.__name__}: {exc}"
            result.error_kind = "internal"
            result.exit_code = exit_code_for(exc)
            self.log.debug("unexpected failure", url=redact_url(result.url), exc_info=exc)

    # -- probe ------------------------------------------------------------ #
    def _probe_with_fallback(self, url: str) -> tuple[ProbeResult, str]:
        chain = self.ctx.pool.order()
        policy = self.settings.retry_policy()
        failures: list[str] = []
        for position, backend_name in enumerate(chain):
            backend = self.ctx.pool.acquire(backend_name)

            def probe_with(backend: DownloadBackend = backend) -> ProbeResult:
                return backend.probe(url)

            try:
                with self.ctx.host_limiter.slot(url):
                    probe = self._retry_call(
                        probe_with,
                        policy=policy,
                        description=f"probe {redact_url(url)}",
                    )
                if position and failures:
                    self.log.info(
                        "backend failover succeeded",
                        backend=backend_name,
                        failed=", ".join(failures),
                    )
                return probe, backend_name
            except (NetworkError, UsageError) as exc:
                failures.append(f"{backend_name}: {exc}")
                if position == len(chain) - 1 or not self.ctx.pool.fallback:
                    raise
                self.log.warning(
                    "backend failed during probe, trying fallback",
                    backend=backend_name,
                    error=str(exc),
                    next=chain[position + 1],
                )
        raise NetworkError("no backend could probe the URL")  # pragma: no cover

    def _retry_call(self, func: Callable[[], Any], *, policy: Any, description: str) -> Any:
        attempt = 1
        while True:
            self.ctx.interrupt.check()
            try:
                return func()
            except Wget2Error as exc:
                if not policy.should_retry(exc) or attempt >= policy.attempts:
                    raise
                delay = policy.delay_for(attempt)
                self.log.warning(
                    "retrying after error",
                    target=description,
                    attempt=attempt,
                    error=str(exc),
                    delay=f"{delay:.2f}s",
                )
                self._sleep(delay)
                attempt += 1

    def _sleep(self, delay: float) -> None:
        deadline = time.monotonic() + max(0.0, delay)
        while True:
            self.ctx.interrupt.check()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(0.25, remaining))

    # -- limiters --------------------------------------------------------- #
    def _limiters(self, connection_limiter: RateLimiter | None = None) -> MultiLimiter:
        limiters = list(self.ctx.global_limiter.limiters)
        if connection_limiter is not None and connection_limiter.enabled:
            limiters.append(connection_limiter)
        return MultiLimiter(limiters)

    def _new_connection_limiter(self) -> RateLimiter | None:
        return self.settings.connection_limiter()

    # -- main flow -------------------------------------------------------- #
    def _execute(
        self,
        url: str,
        index: int,
        probe: ProbeResult,
        backend_name: str,
        result: FileResult,
        started: float,
        name_url: str | None = None,
    ) -> None:
        """Apply size/spider/mode checks, then transfer under a per-path lock."""
        settings = self.settings
        size = probe.size
        effective_name_url = name_url or url

        if settings.max_filesize and size is not None and size > settings.max_filesize:
            raise PolicyError(
                f"resource is {human_size(size)}, above --max-filesize ({human_size(settings.max_filesize)})",
                url=url,
            )
        if size == 0:
            raise PolicyError(f"refusing to download empty resource: {redact_url(url)}")

        if settings.spider or settings.dry_run:
            planned = resolve_output_path(settings.output, probe, effective_name_url, index=index)
            result.path = planned.path
            result.status = "spidered" if settings.spider else "dry-run"
            result.size = size
            result.detail = "{} {} -> {} ({})".format(
                "spider" if settings.spider else "dry-run",
                human_size(size),
                planned.path,
                "ranges" if probe.accept_ranges else "single stream",
            )
            self.log.info(
                "probe complete",
                url=redact_url(url),
                path=str(planned.path),
                size=size,
                ranges=probe.accept_ranges,
            )
            if settings.progress_mode != "none":
                self.ctx.progress.log(result.detail)
            return

        if settings.check_disk and size:
            ensure_free_space(
                resolve_output_path(settings.output, probe, effective_name_url, index=index).path.parent,
                size,
                factor=1.05,
            )

        # a parallel batch may map two URLs onto the same output file: serialise
        # them so they cannot corrupt each other's .part file
        initial = resolve_output_path(settings.output, probe, effective_name_url, index=index)
        with self.ctx.path_locks.hold(initial.path):
            # re-evaluate the existing-file policy now that we own the path
            resolution = resolve_output_path(settings.output, probe, effective_name_url, index=index)
            final_path = resolution.path
            result.path = final_path
            if resolution.skipped:
                result.status = resolution.skip_reason or "skipped"
                result.detail = resolution.detail
                result.exit_code = EXIT_OK
                self.log.info(
                    "skipping file",
                    url=redact_url(url),
                    path=str(final_path),
                    reason=resolution.detail or resolution.skip_reason,
                )
                return
            self._transfer(url, probe, backend_name, result, started, final_path, size)

    def _transfer(
        self,
        url: str,
        probe: ProbeResult,
        backend_name: str,
        result: FileResult,
        started: float,
        final_path: Path,
        size: int | None,
    ) -> None:
        """Download into ``final_path`` (caller holds the per-path lock)."""
        settings = self.settings
        backend = self.ctx.pool.acquire(backend_name)
        ensure_directory(final_path.parent)
        specs = self._checksum_specs(final_path.name)

        connections = settings.effective_connections(size)
        use_ranges = bool(
            probe.accept_ranges
            and size
            and size >= MULTI_MIN_SIZE
            and connections > 1
            and settings.request.method.upper() == "GET"
            and settings.request.data is None
        )
        if use_ranges and size:
            segment_count = min(
                max(connections, connections * SEGMENTS_PER_CONNECTION),
                max(1, size // (256 * 1024)),
            )
            plan = plan_ranges(size, max(1, segment_count))
        else:
            plan = [(0, size)] if size else []

        part_path = final_path.with_name(final_path.name + PART_SUFFIX)
        state, resumed = open_part_state(
            part_path,
            url=url,
            size=size,
            etag=probe.etag,
            last_modified=probe.last_modified,
            plan=plan,
            backend=backend_name,
            resume=settings.continue_,
            allow_unvalidated=settings.allow_unvalidated_resume,
        )
        result.resumed = resumed
        if resumed and not probe.accept_ranges and state.done_bytes():
            self.log.warning(
                "server no longer supports ranges; restarting download",
                url=redact_url(url),
            )
            state.covered = []
            state.plan = [(0, size)] if size else []
            resumed = False
            result.resumed = False

        buffered = bool(size is not None and size <= settings.buffer_threshold)
        mode = "buffered" if buffered else "chunked→disk"
        self.log.info(
            "starting download",
            url=redact_url(url),
            path=str(final_path),
            size=size,
            backend=backend_name,
            connections=connections if use_ranges else 1,
            mode=mode,
            resumed=resumed,
            done_bytes=state.done_bytes() if resumed else 0,
            etag=probe.etag,
        )
        result.connections = (min(connections, len(plan)) if use_ranges else 1) or 1

        handle = self.ctx.progress.add_file(
            final_path.name, size, already_done=state.done_bytes() if resumed else 0
        )
        status = "failed"
        failed = False
        fd = -1
        try:
            if not resumed or not part_path.exists():
                preallocate(part_path, size or 0)
            elif size and part_path.stat().st_size != size:
                preallocate(part_path, size)
            try:
                fd = os.open(part_path, os.O_WRONLY)
            except OSError as exc:
                raise FilesystemError(f"cannot open {part_path} for writing: {exc}") from exc

            bytes_before = state.done_bytes()
            if size is None:
                self._download_single(backend, url, state, size, fd, handle)
            else:
                missing = state.missing() if resumed else plan
                missing = [segment for segment in missing if segment[1] > segment[0]]
                if not missing:
                    self.log.info("partial file already complete", path=str(part_path))
                elif use_ranges and len(missing) > 1:
                    try:
                        self._download_segments(
                            backend, url, state, missing, size, fd, handle, buffered, connections
                        )
                    except RangeNotSupportedError as exc:
                        self.log.warning(
                            "range requests unsupported, falling back to a single stream",
                            url=redact_url(url),
                            error=str(exc),
                        )
                        state.covered = []
                        state.plan = [(0, size)]
                        state.save()
                        os.ftruncate(fd, 0)
                        preallocate(part_path, size)
                        self._download_single(backend, url, state, size, fd, handle)
                else:
                    self._download_single(backend, url, state, size, fd, handle)

            self._verify_complete(state, size, part_path)
            digests = verify_file(part_path, specs) if specs else {}
            result.checksums = digests
            os.fsync(fd)
            os.close(fd)
            fd = -1
            atomic_finalize(part_path, final_path)
            state.discard()
            apply_mtime(final_path, probe.last_modified_ts)
            result.size = size if size is not None else final_path.stat().st_size
            result.bytes_written = max(0, state.done_bytes() - bytes_before)
            status = "complete"
            if settings.manifest:
                write_manifest(
                    final_path,
                    url=url,
                    final_url=probe.final_url,
                    etag=probe.etag,
                    last_modified=probe.last_modified,
                    size=result.size,
                    digests=digests,
                    backend=backend_name,
                )
            elapsed = max(1e-9, time.monotonic() - started)
            self.log.info(
                "download complete",
                url=redact_url(url),
                path=str(final_path),
                size=result.size,
                speed=f"{human_size((result.size or 0) / elapsed)}/s",
            )
            result.status = "downloaded"
        except BaseException:
            failed = True
            raise
        finally:
            if fd >= 0:
                try:
                    os.close(fd)
                except OSError:  # pragma: no cover
                    pass
            if failed:
                self._cleanup_failure(state, part_path)
            self.ctx.progress.finish(handle, status=status)

    # -- range execution -------------------------------------------------- #
    def _download_segments(
        self,
        backend: DownloadBackend,
        url: str,
        state: PartState,
        segments: list[Interval],
        size: int | None,
        fd: int,
        handle: TaskHandle,
        buffered: bool,
        connections: int,
    ) -> None:
        queue: Queue[Interval] = Queue()
        for segment in segments:
            queue.put(segment)
        errors: list[BaseException] = []
        stop = threading.Event()
        workers = max(1, min(connections, len(segments)))

        def worker() -> None:
            limiter = self._new_connection_limiter()
            while not stop.is_set():
                try:
                    segment = queue.get_nowait()
                except Empty:
                    return
                try:
                    self._fetch_segment(backend, url, state, segment, size, fd, handle, buffered, limiter)
                except BaseException as exc:  # noqa: BLE001 - re-raised in the caller
                    stop.set()
                    errors.append(exc)
                    return
                finally:
                    queue.task_done()

        threads = [
            threading.Thread(target=worker, name=f"wget2-range-{index}", daemon=True)
            for index in range(workers)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            while thread.is_alive():
                thread.join(timeout=0.2)
        if errors:
            raise errors[0]

    def _fetch_segment(
        self,
        backend: DownloadBackend,
        url: str,
        state: PartState,
        segment: Interval,
        size: int | None,
        fd: int,
        handle: TaskHandle,
        buffered: bool,
        connection_limiter: RateLimiter | None,
    ) -> None:
        start, end = segment
        policy = self.settings.retry_policy()
        attempt = 1
        while True:
            self.ctx.interrupt.check()
            offset = start if buffered else state.resume_offset(start, end)
            if offset >= end:
                return
            limiters = self._limiters(connection_limiter)
            if buffered:
                sink: MemorySink | DiskSink = MemorySink(
                    progress=self.ctx.progress,
                    handle=handle,
                    limiters=limiters,
                    interrupt=self.ctx.interrupt,
                )
            else:
                sink = DiskSink(
                    fd,
                    offset,
                    state=state,
                    progress=self.ctx.progress,
                    handle=handle,
                    limiters=limiters,
                    interrupt=self.ctx.interrupt,
                )
            try:
                with self.ctx.host_limiter.slot(url):
                    info = backend.stream_range(url, offset, end - 1, sink.feed)
                self._check_ranged_response(info, offset, end, size)
                expected = end - offset
                if sink.written < expected:
                    raise ContentLengthError(
                        f"short read for bytes {offset}-{end - 1}: "
                        f"expected {expected} bytes, got {sink.written}",
                        url=url,
                    )
                if buffered:
                    assert isinstance(sink, MemorySink)
                    self._commit_memory(fd, state, sink, offset)
                return
            except Wget2Error as exc:
                if buffered:
                    assert isinstance(sink, MemorySink)
                    if sink.written:
                        self.ctx.progress.update(handle, advance=-sink.written)
                    sink.reset()
                if not policy.should_retry(exc) or attempt >= policy.attempts:
                    raise
                delay = policy.delay_for(attempt)
                self.log.warning(
                    "retrying segment",
                    url=redact_url(url),
                    segment=f"{start}-{end - 1}",
                    attempt=attempt,
                    error=str(exc),
                    delay=f"{delay:.2f}s",
                )
                self._sleep(delay)
                attempt += 1

    def _commit_memory(self, fd: int, state: PartState, sink: MemorySink, offset: int) -> None:
        data = bytes(sink.buffer)
        try:
            written = 0
            while written < len(data):
                written += os.pwrite(fd, data[written:], offset + written)
            os.fsync(fd)
        except OSError as exc:
            raise FilesystemError(f"write failed at offset {offset}: {exc}") from exc
        state.mark(offset, offset + len(data))
        state.save()

    def _download_single(
        self,
        backend: DownloadBackend,
        url: str,
        state: PartState,
        size: int | None,
        fd: int,
        handle: TaskHandle,
    ) -> None:
        policy = self.settings.retry_policy()
        attempt = 1
        restarted = False
        connection_limiter = self._new_connection_limiter()
        while True:
            self.ctx.interrupt.check()
            offset = state.resume_offset(0, size) if size else 0
            if size and offset >= size:
                return
            sink = DiskSink(
                fd,
                offset,
                state=state,
                progress=self.ctx.progress,
                handle=handle,
                limiters=self._limiters(connection_limiter),
                interrupt=self.ctx.interrupt,
            )
            try:
                with self.ctx.host_limiter.slot(url):
                    info = backend.stream_range(url, offset or None, None, sink.feed)
                if info.status == 200:
                    declared = info.int_header("content-length")
                    if size is None and declared is not None:
                        size = declared
                        state.size = size
                        state.plan = [(0, size)]
                    if size is not None and sink.written < size - offset:
                        raise ContentLengthError(
                            f"incomplete body: expected {size - offset} bytes, got {sink.written}",
                            url=url,
                        )
                return
            except RangeNotSupportedError as exc:
                if restarted or offset == 0:
                    raise
                self.log.warning(
                    "server does not support resume; restarting from byte 0",
                    url=redact_url(url),
                    error=str(exc),
                )
                state.covered = []
                state.save()
                os.ftruncate(fd, 0)
                if size:
                    preallocate(state.part_path, size)
                restarted = True
            except Wget2Error as exc:
                if not policy.should_retry(exc) or attempt >= policy.attempts:
                    raise
                delay = policy.delay_for(attempt)
                self.log.warning(
                    "retrying download",
                    url=redact_url(url),
                    attempt=attempt,
                    error=str(exc),
                    delay=f"{delay:.2f}s",
                )
                self._sleep(delay)
                attempt += 1

    # -- validation ------------------------------------------------------- #
    def _check_ranged_response(self, info: ResponseInfo, start: int, end: int, size: int | None) -> None:
        if info.status == 200:
            if start > 0 or (size is not None and end < size):
                raise RangeNotSupportedError("server ignored the Range header (answered 200)", url=info.url)
            return
        if info.status != 206:
            return
        parsed = parse_content_range(info.header("content-range"))
        if not parsed:
            raise IntegrityError("206 response without a valid Content-Range header", url=info.url)
        range_start, _range_end, total = parsed
        if range_start != start:
            raise IntegrityError(
                f"server returned range starting at {range_start}, expected {start}", url=info.url
            )
        if total is not None and size is not None and total != size:
            raise IntegrityError(f"resource size changed during download ({size} -> {total})", url=info.url)

    def _verify_complete(self, state: PartState, size: int | None, part_path: Path) -> None:
        if size is None:
            return
        if not state.complete():
            missing = state.missing()
            first = missing[0] if missing else (0, 0)
            raise ContentLengthError(
                f"incomplete download: {size - state.done_bytes()} bytes missing (first gap at {first[0]})",
                url=state.url,
            )
        actual = part_path.stat().st_size
        if actual < size:  # pragma: no cover - defensive
            raise ContentLengthError(f"file on disk is {actual} bytes, expected {size}", url=state.url)

    def _cleanup_failure(self, state: PartState, part_path: Path) -> None:
        keep = self.settings.keep_partial and not self.settings.remove_on_error
        if not keep:
            safe_unlink(part_path)
            state.discard()
            self.log.info("removed partial file", path=str(part_path))
            return
        try:
            state.save()
        except Wget2Error:  # pragma: no cover - best effort
            pass
        if part_path.exists():
            self.log.info(
                "kept partial file for resume",
                path=str(part_path),
                hint="use --continue/-C to resume",
            )

    # -- helpers ---------------------------------------------------------- #
    def _checksum_specs(self, filename: str) -> list[ChecksumSpec]:
        specs = list(self.settings.checksums)
        if self.settings.checksum_table:
            specs.extend(specs_for(filename, self.settings.checksum_table))
        return specs
