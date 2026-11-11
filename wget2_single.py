#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import random
import re
import shutil
import signal
import string
import sys
import threading
import time
import unicodedata
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field, replace
from email.utils import parsedate_to_datetime
from fnmatch import fnmatch
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Optional
from urllib.parse import unquote, urljoin, urlparse

import pycurl
import typer
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TransferSpeedColumn,
)

__version__ = "2.0.0-onefile"

EXIT_OK = 0
EXIT_INTERNAL = 1
EXIT_USAGE = 2
EXIT_NETWORK = 3
EXIT_HTTP = 4
EXIT_INTEGRITY = 5
EXIT_FILESYSTEM = 6
EXIT_BATCH = 7
EXIT_INTERRUPTED = 130

PART_SUFFIX = ".part"
MANIFEST_SUFFIX = ".wget2.json"
STATE_VERSION = 1
MULTI_RANGE_MIN = 1024 * 1024
CHUNK_THRESHOLD = 10 * 1024 * 1024
SEGMENTS_PER_CONNECTION = 4
MAX_CONNECTIONS_PER_FILE = 32
MIN_RANGE_SIZE = 64 * 1024
SYNC_INTERVAL = 8 * 1024 * 1024
MAX_FILENAME = 180
ALLOWED_SCHEMES = frozenset({"http", "https"})
OPT_IN_SCHEMES = frozenset({"file", "ftp", "ftps"})
DEFAULT_RETRY_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})
RETRYABLE_CURL_CODES = frozenset({6, 7, 16, 18, 28, 35, 52, 55, 56, 92})
SUPPORTED_ALGORITHMS = frozenset({"md5", "sha1", "sha224", "sha256", "sha384", "sha512", "blake2b", "sha3_256"})
TEMPLATE_FIELDS = frozenset({"url", "host", "path", "name", "filename", "stem", "ext", "index", "size", "date"})

Interval = tuple[int, int]
Sink = Callable[[bytes], None]

_SIZE_RE = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([kmgtp]?)(i?b?)\s*$", re.IGNORECASE)
_RATE_RE = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([kmgtp]?)(i?b?)?(?:/s|ps)?\s*$", re.IGNORECASE)
_UNITS = {"": 0, "k": 1, "m": 2, "g": 3, "t": 4, "p": 5}
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f-\x9f]")
_UNSAFE_CHARS_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_CONTENT_RANGE_RE = re.compile(r"bytes\s+(\d+)-(\d+)/(\d+|\*)", re.IGNORECASE)
_HEX_RE = re.compile(r"^[0-9a-fA-F]+$")
_CREDENTIAL_URL_RE = re.compile(r"(?i)\b([a-z][a-z0-9+.\-]*://)([^/\s:@]+)(?::([^/\s@]*))?@")
_AUTH_HEADER_RE = re.compile(r"(?i)\b(authorization|proxy-authorization)\b(\s*[:=]\s*)([^\s,;]+(?:\s+[^\s,;]+)?)")
_BEARER_RE = re.compile(r"(?i)\b(bearer|basic|token)(\s+)([A-Za-z0-9\-._~+/=]{4,})")
_SECRET_KV_RE = re.compile(r"(?i)\b(password|passwd|secret|token|api[_-]?key|client[_-]?secret)(\s*[=:]\s*)(\S+)")
_LINK_RE = re.compile(r"""(?:href|src)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))""", re.IGNORECASE)
_WINDOWS_RESERVED = frozenset(
    {"con", "prn", "aux", "nul"} | {f"com{i}" for i in range(1, 10)} | {f"lpt{i}" for i in range(1, 10)}
)


class Wget2Error(Exception):
    kind: str = "error"
    exit_code: int = EXIT_INTERNAL
    retryable: bool = False

    def __init__(self, message: str, *, url: str | None = None, retryable: bool | None = None) -> None:
        super().__init__(message)
        self.url = url
        if retryable is not None:
            self.retryable = retryable


class UsageError(Wget2Error):
    kind = "usage"
    exit_code = EXIT_USAGE


class ConfigError(Wget2Error):
    kind = "config"
    exit_code = EXIT_USAGE


class PolicyError(Wget2Error):
    kind = "policy"
    exit_code = EXIT_USAGE


class NetworkError(Wget2Error):
    kind = "network"
    exit_code = EXIT_NETWORK
    retryable = True


class TransportError(NetworkError):
    kind = "transport"


class RequestTimeout(NetworkError):
    kind = "timeout"


class HTTPStatusError(Wget2Error):
    kind = "http"
    exit_code = EXIT_HTTP

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        url: str | None = None,
        retryable: bool | None = None,
    ) -> None:
        super().__init__(message, url=url, retryable=retryable)
        self.status_code = status_code
        if retryable is None:
            self.retryable = status_code in DEFAULT_RETRY_STATUS


class IntegrityError(Wget2Error):
    kind = "integrity"
    exit_code = EXIT_INTEGRITY


class ChecksumError(IntegrityError):
    kind = "checksum"


class FilesystemError(Wget2Error):
    kind = "filesystem"
    exit_code = EXIT_FILESYSTEM


class Interrupted(Wget2Error):
    kind = "interrupted"
    exit_code = EXIT_INTERRUPTED


class RangeNotSupportedError(NetworkError):
    kind = "range-unsupported"
    retryable = False


class StopStream(Exception):
    pass


def exit_code_for(exc: BaseException) -> int:
    if isinstance(exc, Wget2Error):
        return exc.exit_code
    if isinstance(exc, OSError):
        return EXIT_FILESYSTEM
    return EXIT_INTERNAL


class InterruptFlag:
    def __init__(self) -> None:
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._count = 0

    def set(self) -> int:
        with self._lock:
            self._count += 1
            count = self._count
        self._event.set()
        return count

    def clear(self) -> None:
        with self._lock:
            self._count = 0
        self._event.clear()

    def is_set(self) -> bool:
        return self._event.is_set()

    def check(self) -> None:
        if self._event.is_set():
            raise Interrupted("interrupted by user")


def redact_url(url: str) -> str:
    def _replace(match: re.Match[str]) -> str:
        return f"{match.group(1)}***@{'' if match.group(3) is None else ''}"

    return _CREDENTIAL_URL_RE.sub(_replace, url)


def redact_text(text: str) -> str:
    text = _CREDENTIAL_URL_RE.sub(lambda m: f"{m.group(1)}***@", text)
    text = _AUTH_HEADER_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}***", text)
    text = _BEARER_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}***", text)
    return _SECRET_KV_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}***", text)


_LEVELS = {"debug": 10, "info": 20, "warning": 30, "error": 40, "critical": 50}
_LEVEL_LABEL = {10: "debug", 20: "info", 30: "warning", 40: "error", 50: "critical"}


class Logger:
    def __init__(
        self,
        level: str = "info",
        *,
        path: Path | None = None,
        quiet: bool = False,
        json_logs: bool = False,
    ) -> None:
        self.threshold = _LEVELS.get(level.lower(), 20)
        self.quiet = quiet
        self.json_logs = json_logs
        self._lock = threading.Lock()
        self._handle: io.TextIOWrapper | None = None
        self.line_hook: Callable[[], None] | None = None
        if path is not None:
            ensure_directory(path.parent)
            try:
                self._handle = path.open("a", encoding="utf-8")
            except OSError as exc:
                raise FilesystemError(f"cannot open log file {path}: {exc}") from exc

    def close(self) -> None:
        if self._handle is not None:
            with suppress(OSError):
                self._handle.close()
            self._handle = None

    def _format(self, level: int, message: str, fields: Mapping[str, Any]) -> str:
        if self.json_logs:
            payload: dict[str, Any] = {
                "time": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
                "level": _LEVEL_LABEL.get(level, "info"),
                "message": redact_text(message),
            }
            payload.update(
                {
                    key: value if isinstance(value, (int, float, bool)) or value is None else redact_text(str(value))
                    for key, value in fields.items()
                }
            )
            return json.dumps(payload, default=str)
        parts = [time.strftime("%H:%M:%S"), _LEVEL_LABEL.get(level, "info").ljust(7), message]
        parts.extend(f"{key}={redact_text(str(value))}" for key, value in fields.items())
        return " ".join(parts)

    def emit(self, level: int, message: str, **fields: Any) -> None:
        if level < self.threshold:
            return
        line = self._format(level, message, fields)
        if self.line_hook is not None and (not self.quiet or level >= 40):
            self.line_hook()
        with self._lock:
            if self._handle is not None:
                self._handle.write(line + "\n")
                self._handle.flush()
            if not self.quiet or level >= 40:
                print(line, file=sys.stderr, flush=True)

    def debug(self, message: str, **fields: Any) -> None:
        self.emit(10, message, **fields)

    def info(self, message: str, **fields: Any) -> None:
        self.emit(20, message, **fields)

    def warning(self, message: str, **fields: Any) -> None:
        self.emit(30, message, **fields)

    def error(self, message: str, **fields: Any) -> None:
        self.emit(40, message, **fields)


def human_size(value: float | int | None) -> str:
    if value is None:
        return "unknown"
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(size) < 1024 or unit == "TiB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{size:.1f} TiB"


def parse_size(value: str | int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    match = _SIZE_RE.match(str(value))
    if not match:
        raise UsageError(f"invalid size: {value!r}")
    number, unit, _ = match.groups()
    return int(float(number) * (1024 ** _UNITS[unit.lower()]))


def parse_rate(value: str | float | int | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) or None
    match = _RATE_RE.match(str(value))
    if not match:
        raise UsageError(f"invalid rate: {value!r}")
    number, unit, _ = match.groups()
    rate = float(number) * (1024 ** _UNITS[(unit or "").lower()])
    return rate or None


def parse_int_list(value: str | Sequence[int] | None) -> frozenset[int]:
    if value is None:
        return frozenset()
    if isinstance(value, str):
        items: list[str] = [item.strip() for item in value.replace(";", ",").split(",")]
        return frozenset(int(item) for item in items if item)
    return frozenset(int(item) for item in value)


def parse_http_date(value: str | None) -> float | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return parsed.timestamp()


def parse_content_range(value: str | None) -> tuple[int, int, int | None] | None:
    if not value:
        return None
    match = _CONTENT_RANGE_RE.search(value)
    if not match:
        return None
    start, end, total = match.groups()
    return int(start), int(end), (int(total) if total != "*" else None)


def format_duration(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, rest = divmod(seconds, 60)
    return f"{int(minutes)}m{rest:04.1f}s"


def url_host(url: str) -> str | None:
    try:
        return urlparse(url).hostname
    except ValueError:
        return None


def url_basename(url: str) -> str:
    path = urlparse(url).path
    return unquote(path.rsplit("/", 1)[-1])


def sanitize_filename(name: str, *, fallback: str = "download", max_length: int = MAX_FILENAME) -> str:
    if not name:
        return fallback
    name = unquote(str(name))
    name = name.replace("\\", "/").rsplit("/", 1)[-1]
    name = _CONTROL_RE.sub("", name)
    name = unicodedata.normalize("NFC", name)
    name = _UNSAFE_CHARS_RE.sub("_", name)
    name = name.strip().strip(".").strip()
    if not name or name in {".", ".."}:
        return fallback
    stem, dot, suffix = name.rpartition(".")
    if (dot and stem and stem.lower() in _WINDOWS_RESERVED) or (not dot and name.lower() in _WINDOWS_RESERVED):
        name = f"_{name}"
    if len(name) > max_length:
        stem, dot, suffix = name.rpartition(".")
        if dot and len(suffix) <= 12:
            name = f"{stem[: max_length - len(suffix) - 1]}.{suffix}"
        else:
            name = name[:max_length]
    return name or fallback


def filename_from_url(url: str, *, fallback: str = "download") -> str:
    base = url_basename(url)
    if not base:
        host = url_host(url) or "download"
        if urlparse(url).path.endswith("/"):
            return sanitize_filename(f"{host}-index.html", fallback=fallback)
        return sanitize_filename(f"{host}-index", fallback=fallback)
    return sanitize_filename(base, fallback=fallback)


def content_disposition_filename(header: str | None) -> str | None:
    if not header:
        return None
    match = re.search(r"filename\*\s*=\s*([^;]+)", header, re.IGNORECASE)
    if match:
        raw = match.group(1).strip().strip('"')
        if "''" in raw:
            raw = raw.split("''", 1)[1]
        with suppress(Exception):
            return sanitize_filename(unquote(raw))
    match = re.search(r'filename\s*=\s*"([^"]*)"', header, re.IGNORECASE)
    if not match:
        match = re.search(r"filename\s*=\s*([^;]+)", header, re.IGNORECASE)
    if match:
        raw = match.group(1).strip().strip('"')
        if raw:
            return sanitize_filename(unquote(raw))
    return None


def validate_url(url: str, *, allow_extra_schemes: bool = False) -> str:
    text = (url or "").strip()
    if not text:
        raise UsageError("empty URL")
    try:
        parsed = urlparse(text)
    except ValueError as exc:
        raise UsageError(f"invalid URL {text!r}: {exc}") from exc
    scheme = (parsed.scheme or "").lower()
    if not scheme:
        raise UsageError(f"URL without a scheme: {text!r}")
    allowed = ALLOWED_SCHEMES | (OPT_IN_SCHEMES if allow_extra_schemes else frozenset())
    if scheme not in allowed:
        raise PolicyError(
            f"scheme '{scheme}://' is not allowed (allowed: {', '.join(sorted(allowed))})",
            url=redact_url(text),
        )
    if not parsed.netloc:
        raise UsageError(f"URL without a host: {text!r}")
    return text


def safe_unlink(path: Path) -> None:
    with suppress(OSError):
        path.unlink()


def ensure_directory(path: Path) -> None:
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise FilesystemError(f"cannot create directory {path}: {exc}") from exc


def fsync_dir(path: Path) -> None:
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def preallocate(path: Path, size: int) -> None:
    try:
        with path.open("wb") as handle:
            if size > 0:
                handle.truncate(size)
    except OSError as exc:
        raise FilesystemError(f"cannot allocate {path}: {exc}") from exc


def atomic_finalize(part_path: Path, final_path: Path) -> None:
    ensure_directory(final_path.parent)
    try:
        part_path.replace(final_path)
    except OSError as exc:
        raise FilesystemError(f"cannot finalise {final_path}: {exc}") from exc
    fsync_dir(final_path.parent)


def apply_mtime(path: Path, timestamp: float | None) -> None:
    if timestamp is None:
        return
    with suppress(OSError):
        os.utime(path, (timestamp, timestamp))


def unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem, suffix = path.stem, path.suffix
    for counter in range(1, 10000):
        candidate = path.with_name(f"{stem}.{counter}{suffix}")
        if not candidate.exists():
            return candidate
    raise FilesystemError(f"cannot find a free filename for {path}")


def ensure_free_space(directory: Path, needed: int, *, factor: float = 1.05) -> None:
    with suppress(OSError):
        directory.mkdir(parents=True, exist_ok=True)
    try:
        usage = shutil.disk_usage(directory)
    except OSError:
        return
    required = int(needed * factor)
    if usage.free < required:
        raise FilesystemError(
            f"not enough free space in {directory}: "
            f"{human_size(usage.free)} available, {human_size(required)} needed"
        )


def parse_netrc(path: Path | None = None) -> dict[str, tuple[str, str]]:
    candidate = path or (Path.home() / ".netrc")
    try:
        text = candidate.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        raise ConfigError(f"cannot read netrc file {candidate}: {exc}") from exc
    entries: dict[str, tuple[str, str]] = {}
    tokens = text.split()
    index = 0
    machine = ""
    login = ""
    password = ""
    while index < len(tokens):
        token = tokens[index]
        following = tokens[index + 1] if index + 1 < len(tokens) else ""
        if token in {"machine", "default"}:
            if machine and login:
                entries[machine] = (login, password)
            machine = "" if token == "default" else following
            login, password = "", ""
            index += 2
        elif token == "login":
            login = following
            index += 2
        elif token == "password":
            password = following
            index += 2
        else:
            index += 1
    if machine and login:
        entries[machine] = (login, password)
    return entries


def plan_ranges(size: int, connections: int, *, min_range: int = MIN_RANGE_SIZE) -> list[Interval]:
    if size <= 0:
        return []
    connections = max(1, int(connections))
    max_useful = max(1, min(connections, size // max(1, min_range)))
    count = max(1, min(connections, size, max_useful))
    base, remainder = divmod(size, count)
    ranges: list[Interval] = []
    offset = 0
    for index in range(count):
        length = base + (1 if index < remainder else 0)
        ranges.append((offset, offset + length))
        offset += length
    return ranges


def merge_intervals(intervals: Iterable[Interval]) -> list[Interval]:
    ordered = sorted((int(start), int(end)) for start, end in intervals if end > start)
    merged: list[Interval] = []
    for start, end in ordered:
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def covered_bytes(intervals: Iterable[Interval]) -> int:
    return sum(end - start for start, end in merge_intervals(intervals))


def subtract_interval(interval: Interval, covered: Iterable[Interval]) -> list[Interval]:
    start, end = interval
    missing: list[Interval] = []
    cursor = start
    for cov_start, cov_end in merge_intervals(covered):
        if cov_end <= cursor:
            continue
        if cov_start >= end:
            break
        if cov_start > cursor:
            missing.append((cursor, min(cov_start, end)))
        cursor = max(cursor, cov_end)
        if cursor >= end:
            break
    if cursor < end:
        missing.append((cursor, end))
    return missing


def split_segments(gaps: Sequence[Interval], target: int) -> list[Interval]:
    total = sum(end - start for start, end in gaps)
    if not gaps or target <= 1 or total <= 0:
        return list(gaps)
    ideal = max(MIN_RANGE_SIZE, total // target)
    segments: list[Interval] = []
    for start, end in gaps:
        cursor = start
        while cursor < end:
            step = min(end, cursor + ideal)
            segments.append((cursor, step))
            cursor = step
    return segments


@dataclass
class PartState:
    part_path: Path
    url: str
    size: int | None
    etag: str | None = None
    last_modified: str | None = None
    plan: list[Interval] = field(default_factory=list)
    covered: list[Interval] = field(default_factory=list)
    created: float = field(default_factory=time.time)
    updated: float = field(default_factory=time.time)
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False, compare=False)
    _dirty: bool = field(default=False, repr=False, compare=False)
    _last_save: float = field(default=0.0, repr=False, compare=False)

    @property
    def state_path(self) -> Path:
        return self.part_path.with_name(self.part_path.name + ".json")

    @classmethod
    def load(cls, part_path: Path) -> PartState | None:
        state_path = part_path.with_name(part_path.name + ".json")
        if not state_path.exists():
            return None
        try:
            payload = json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            safe_unlink(state_path)
            return None
        if payload.get("version") != STATE_VERSION:
            return None
        try:
            return cls(
                part_path=part_path,
                url=payload.get("url", ""),
                size=payload.get("size"),
                etag=payload.get("etag"),
                last_modified=payload.get("last_modified"),
                plan=[(int(item[0]), int(item[1])) for item in payload.get("plan", [])],
                covered=[(int(item[0]), int(item[1])) for item in payload.get("covered", [])],
                created=payload.get("created", time.time()),
                updated=payload.get("updated", time.time()),
            )
        except (TypeError, ValueError):
            safe_unlink(state_path)
            return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": STATE_VERSION,
            "url": redact_url(self.url),
            "size": self.size,
            "etag": self.etag,
            "last_modified": self.last_modified,
            "plan": [list(item) for item in self.plan],
            "covered": [list(item) for item in self.covered],
            "created": self.created,
            "updated": self.updated,
        }

    def save(self, *, force: bool = True) -> None:
        with self._lock:
            if not force and not self._dirty:
                return
            self.updated = time.time()
            payload = json.dumps(self.to_dict(), indent=1)
            self._dirty = False
            self._last_save = time.monotonic()
        state_path = self.state_path
        tmp = state_path.with_name(f"{state_path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        try:
            state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp.write_text(payload, encoding="utf-8")
            tmp.replace(state_path)
        except OSError as exc:
            raise FilesystemError(f"cannot write resume state {state_path}: {exc}") from exc
        finally:
            safe_unlink(tmp)

    def maybe_save(self, interval: float = 2.0) -> None:
        with self._lock:
            due = self._dirty and (time.monotonic() - self._last_save) >= interval
        if due:
            self.save()

    def discard(self) -> None:
        safe_unlink(self.state_path)
        fsync_dir(self.state_path.parent)

    def mark(self, start: int, end: int, *, interval: float = 2.0) -> None:
        if end <= start:
            return
        with self._lock:
            self.covered = merge_intervals([*self.covered, (start, end)])
            self._dirty = True
        self.maybe_save(interval)

    def resume_offset(self, start: int, end: int) -> int:
        with self._lock:
            covered = list(self.covered)
        offset = start
        for cov_start, cov_end in covered:
            if cov_end <= offset:
                continue
            if cov_start > offset:
                break
            offset = max(offset, min(cov_end, end))
        return offset

    def missing(self) -> list[Interval]:
        if self.size is None:
            return []
        with self._lock:
            covered = list(self.covered)
            plan = list(self.plan) or [(0, self.size)]
        missing: list[Interval] = []
        for interval in plan:
            missing.extend(subtract_interval(interval, covered))
        return missing

    def done_bytes(self) -> int:
        with self._lock:
            return covered_bytes(self.covered)

    def matches_remote(
        self,
        *,
        size: int | None,
        etag: str | None,
        last_modified: str | None,
        allow_unvalidated: bool = False,
    ) -> tuple[bool, str]:
        if self.size is None:
            return False, "previous download had unknown size"
        if size is not None and size != self.size:
            return False, f"size changed ({self.size} -> {size})"
        previous_etag = (self.etag or "").removeprefix("W/").strip('"')
        current_etag = (etag or "").removeprefix("W/").strip('"')
        if previous_etag and current_etag and previous_etag != current_etag:
            return False, f"ETag changed ({previous_etag} -> {current_etag})"
        if self.last_modified and last_modified and self.last_modified != last_modified:
            return False, "Last-Modified changed"
        if not previous_etag and not current_etag and not (self.last_modified and last_modified):
            if allow_unvalidated:
                return True, "resuming without validators (--continue forced)"
            return False, "no ETag/Last-Modified to validate the partial file"
        return True, "validators match"


def open_part_state(
    part_path: Path,
    *,
    url: str,
    size: int | None,
    etag: str | None,
    last_modified: str | None,
    plan: list[Interval],
    resume: bool,
    allow_unvalidated: bool,
    log: Logger,
) -> tuple[PartState, bool]:
    existing = PartState.load(part_path) if resume else None
    if existing is not None and not part_path.exists():
        existing.discard()
        existing = None
    if existing is not None and existing.url and url and existing.url != url:
        log.warning("restarting download", path=str(part_path), reason="different URL")
        existing.discard()
        existing = None
    if existing is not None:
        ok, reason = existing.matches_remote(
            size=size, etag=etag, last_modified=last_modified, allow_unvalidated=allow_unvalidated
        )
        if ok:
            log.info(
                "resuming partial download",
                path=str(part_path),
                resume=reason,
                done_bytes=existing.done_bytes(),
                size=size,
            )
            existing.url = url
            return existing, True
        log.warning("restarting download", path=str(part_path), reason=reason)
        existing.discard()
    return (
        PartState(
            part_path=part_path,
            url=url,
            size=size,
            etag=etag,
            last_modified=last_modified,
            plan=[(int(start), int(end)) for start, end in plan],
            covered=[],
        ),
        False,
    )


class TokenBucket:
    def __init__(self, rate: float, burst: float | None = None) -> None:
        if rate <= 0:
            raise UsageError("rate limit must be positive")
        self.rate = float(rate)
        self.capacity = float(burst if burst is not None else max(rate, 1.0))
        self._tokens = self.capacity
        self._updated = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        rate = self.rate
        now = time.monotonic()
        elapsed = now - self._updated
        if elapsed > 0:
            self._tokens = min(self.capacity, self._tokens + elapsed * rate)
            self._updated = now

    def consume(self, amount: int) -> None:
        if amount <= 0:
            return
        remaining = float(amount)
        while remaining > 0:
            with self._lock:
                self._refill()
                take = min(remaining, self._tokens)
                self._tokens -= take
                remaining -= take
                if remaining > 0:
                    deficit = remaining - self._tokens
                    wait = max(deficit / self.rate, 0.001)
                else:
                    wait = 0.0
            if wait > 0:
                time.sleep(min(wait, 0.5))


class MultiLimiter:
    def __init__(self, limiters: Sequence[TokenBucket] = ()) -> None:
        self._limiters = [limiter for limiter in limiters if limiter is not None]

    @property
    def enabled(self) -> bool:
        return bool(self._limiters)

    def consume(self, amount: int) -> None:
        for limiter in self._limiters:
            limiter.consume(amount)


@dataclass(frozen=True)
class RetryPolicy:
    max_retries: int = 5
    delay: float = 1.0
    backoff: float = 2.0
    max_delay: float = 60.0
    jitter: float = 0.25
    retry_status: frozenset[int] = DEFAULT_RETRY_STATUS

    @property
    def attempts(self) -> int:
        return max(0, self.max_retries) + 1

    def delay_for(self, attempt: int) -> float:
        base = min(self.max_delay, max(0.0, self.delay) * (self.backoff ** max(0, attempt - 1)))
        if self.jitter > 0 and base > 0:
            spread = base * self.jitter
            base = random.uniform(max(0.0, base - spread), base + spread)
        return max(0.0, base)

    def should_retry(self, exc: BaseException) -> bool:
        if isinstance(exc, HTTPStatusError):
            return (exc.status_code or 0) in self.retry_status or exc.retryable
        if isinstance(exc, RangeNotSupportedError):
            return False
        if isinstance(exc, NetworkError):
            return exc.retryable
        return False


@dataclass(frozen=True)
class ChecksumSpec:
    algorithm: str
    expected: str
    source: str = "cli"

    def __str__(self) -> str:
        return f"{self.algorithm}:{self.expected}"


def parse_checksum(value: str, *, source: str = "cli") -> ChecksumSpec:
    text = (value or "").strip()
    if not text:
        raise UsageError("empty checksum value")
    algorithm = "sha256"
    digest = text
    for separator in (":", "="):
        if separator in text:
            head, _, tail = text.partition(separator)
            if head.strip().lower() in SUPPORTED_ALGORITHMS:
                algorithm, digest = head.strip().lower(), tail.strip()
                break
    digest = digest.strip().lower()
    if not _HEX_RE.match(digest):
        raise UsageError(f"invalid checksum digest (expected hex): {value!r}")
    expected_length = hashlib.new(algorithm).digest_size * 2
    if len(digest) != expected_length:
        raise UsageError(f"{algorithm} digest must be {expected_length} hex chars, got {len(digest)}")
    return ChecksumSpec(algorithm=algorithm, expected=digest, source=source)


def parse_checksums(values: Sequence[str] | None) -> list[ChecksumSpec]:
    return [parse_checksum(value) for value in (values or ()) if value and value.strip()]


def load_checksum_file(path: Path) -> dict[str, list[ChecksumSpec]]:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        raise ConfigError(f"cannot read checksum file {path}: {exc}") from exc
    table: dict[str, list[ChecksumSpec]] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        digest, _, name = stripped.partition(" ")
        name = name.strip().lstrip("*")
        if not name:
            parts = stripped.split()
            if len(parts) < 2:
                continue
            digest, name = parts[0], parts[1].lstrip("*")
        algorithm = "sha256"
        if ":" in digest:
            head, _, tail = digest.partition(":")
            if head.lower() in SUPPORTED_ALGORITHMS:
                algorithm, digest = head.lower(), tail
        try:
            spec = parse_checksum(f"{algorithm}:{digest}", source=str(path))
        except UsageError:
            continue
        table.setdefault(name, []).append(spec)
    return table


def digest_file(path: Path, algorithm: str) -> str:
    digest = hashlib.new(algorithm)
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise FilesystemError(f"cannot read {path} for checksumming: {exc}") from exc
    return digest.hexdigest()


def verify_file(path: Path, specs: Sequence[ChecksumSpec]) -> dict[str, str]:
    results: dict[str, str] = {}
    for spec in specs:
        actual = digest_file(path, spec.algorithm)
        results[spec.algorithm] = actual
        if actual != spec.expected:
            raise ChecksumError(f"{spec.algorithm} mismatch for {path.name}: expected {spec.expected}, got {actual}")
    return results


@dataclass(frozen=True)
class Response:
    status: int
    url: str
    headers: Mapping[str, str] = field(default_factory=dict)

    def header(self, name: str, default: str | None = None) -> str | None:
        return self.headers.get(name.lower(), default)

    def int_header(self, name: str) -> int | None:
        value = self.header(name)
        if value is None:
            return None
        try:
            return int(value.split(",")[0].strip())
        except ValueError:
            return None


@dataclass(frozen=True)
class ProbeResult:
    url: str
    final_url: str
    status: int
    size: int | None = None
    etag: str | None = None
    last_modified: str | None = None
    accept_ranges: bool = False
    suggested_filename: str | None = None
    headers: Mapping[str, str] = field(default_factory=dict)

    def header(self, name: str, default: str | None = None) -> str | None:
        return self.headers.get(name.lower(), default)

    @property
    def last_modified_ts(self) -> float | None:
        return parse_http_date(self.last_modified)


class CookieJar:
    def __init__(self) -> None:
        self._cookies: dict[str, dict[str, str]] = {}
        self._lock = threading.Lock()

    def update(self, url: str, set_cookie_headers: Sequence[str]) -> None:
        host = url_host(url)
        if not host:
            return
        for raw in set_cookie_headers:
            pair = raw.split(";", 1)[0].strip()
            if not pair or "=" not in pair:
                continue
            name, _, value = pair.partition("=")
            domain = host
            match = re.search(r"(?i)\bdomain\s*=\s*([^;]+)", raw)
            if match:
                domain = match.group(1).strip().lstrip(".").lower() or host
            with self._lock:
                self._cookies.setdefault(domain, {})[name.strip()] = value.strip()

    def header_for(self, url: str) -> str | None:
        host = (url_host(url) or "").lower()
        if not host:
            return None
        with self._lock:
            entries = dict(self._cookies)
        values: list[str] = []
        for domain, cookies in entries.items():
            if host == domain or host.endswith("." + domain) or host.endswith(domain):
                values.extend(f"{name}={value}" for name, value in cookies.items())
        return "; ".join(values) if values else None

    def load_file(self, path: Path) -> None:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            raise ConfigError(f"cannot read cookie file {path}: {exc}") from exc
        for line in text.splitlines():
            if not line.strip() or line.startswith("#"):
                continue
            fields = line.split("\t")
            if len(fields) < 7:
                continue
            domain, _flag, _path, _secure, _expires, name, value = fields[:7]
            with self._lock:
                self._cookies.setdefault(domain.lstrip(".").lower(), {})[name] = value

    def save(self, path: Path) -> None:
        with self._lock:
            entries = {domain: dict(values) for domain, values in self._cookies.items()}
        ensure_directory(path.parent)
        lines = ["# Netscape HTTP Cookie File", ""]
        for domain, cookies in entries.items():
            for name, value in cookies.items():
                lines.append(f"{domain}\tTRUE\t/\tFALSE\t0\t{name}\t{value}")
        try:
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        except OSError as exc:
            raise FilesystemError(f"cannot write cookie jar {path}: {exc}") from exc


@dataclass(frozen=True)
class RequestOptions:
    timeout: float = 30.0
    connect_timeout: float = 10.0
    max_redirects: int = 10
    insecure: bool = False
    ca_cert: Path | None = None
    client_cert: Path | None = None
    client_key: Path | None = None
    user_agent: str | None = None
    referer: str | None = None
    headers: tuple[tuple[str, str], ...] = ()
    user: str | None = None
    password: str | None = None
    bearer_token: str | None = None
    netrc: Path | None = None
    method: str = "GET"
    data: bytes | None = None
    proxy: str | None = None
    no_proxy: bool = False
    http2: bool = True
    max_filesize: int | None = None

    def with_credentials(self) -> RequestOptions:
        if self.user or not self.netrc:
            return self
        entries = parse_netrc(self.netrc)
        if not entries:
            return self
        login, password = next(iter(entries.values()))
        return replace(self, user=login, password=password)

    def credentials_for(self, url: str) -> tuple[str | None, str | None]:
        if self.user:
            return self.user, self.password
        if not self.netrc:
            return None, None
        entries = parse_netrc(self.netrc)
        host = (url_host(url) or "").lower()
        if host in entries:
            return entries[host]
        if "" in entries:
            return entries[""]
        return None, None


def _map_curl_error(exc: pycurl.error, url: str, *, max_filesize: int | None = None) -> Wget2Error:
    args = exc.args
    code = int(args[0]) if args else -1
    message = str(args[1]) if len(args) > 1 else str(exc)
    if code == 63:
        return PolicyError(
            f"resource exceeds --max-filesize ({human_size(max_filesize)})",
            url=redact_url(url),
        )
    if code == 28:
        return RequestTimeout(f"timeout: {message}", url=redact_url(url))
    return TransportError(
        f"curl error {code}: {message}",
        url=redact_url(url),
        retryable=code in RETRYABLE_CURL_CODES,
    )


class HttpClient:
    def __init__(
        self,
        request: RequestOptions,
        log: Logger,
        cookies: CookieJar | None = None,
        retry_status: frozenset[int] = DEFAULT_RETRY_STATUS,
    ) -> None:
        self.request_options = request
        self.log = log
        self.cookies = cookies
        self.retry_status = retry_status

    def _build_headers(self, url: str, extra: Sequence[tuple[str, str]]) -> list[str]:
        options = self.request_options
        merged: list[tuple[str, str]] = []
        if options.user_agent:
            merged.append(("User-Agent", options.user_agent))
        if options.referer:
            merged.append(("Referer", options.referer))
        if options.bearer_token:
            merged.append(("Authorization", f"Bearer {options.bearer_token}"))
        if self.cookies is not None:
            cookie_header = self.cookies.header_for(url)
            if cookie_header:
                merged.append(("Cookie", cookie_header))
        merged.extend(options.headers)
        merged.extend(extra)
        return [f"{name}: {value}" for name, value in merged]

    def request(
        self,
        url: str,
        *,
        method: str | None = None,
        headers: Sequence[tuple[str, str]] = (),
        sink: Sink | None = None,
        allow_status: frozenset[int] = frozenset(),
        byte_range: tuple[int, int | None] | None = None,
        enforce_max_filesize: bool = False,
    ) -> Response:
        options = self.request_options
        effective_method = (method or options.method or "GET").upper()
        collected: list[tuple[str, str]] = []
        state: dict[str, Any] = {"status": 0, "stop": False, "error": None}

        def on_header(line: bytes) -> int:
            text = line.decode("iso-8859-1").rstrip("\r\n")
            if not text:
                return len(line)
            if text.startswith("HTTP/"):
                parts = text.split()
                if len(parts) >= 2 and parts[1].isdigit():
                    state["status"] = int(parts[1])
                    if int(parts[1]) >= 200:
                        collected.clear()
                return len(line)
            if ":" in text:
                name, value = text.split(":", 1)
                collected.append((name.strip(), value.strip()))
            return len(line)

        def on_write(data: bytes) -> int:
            if state["status"] >= 400:
                return len(data)
            if sink is not None:
                try:
                    sink(data)
                except StopStream:
                    state["stop"] = True
                    return 0
                except BaseException as exc:
                    state["error"] = exc
                    return 0
            return len(data)

        curl = pycurl.Curl()
        try:
            curl.setopt(pycurl.URL, url)
            curl.setopt(pycurl.NOSIGNAL, 1)
            curl.setopt(pycurl.FOLLOWLOCATION, 1 if options.max_redirects else 0)
            curl.setopt(pycurl.MAXREDIRS, max(0, options.max_redirects))
            curl.setopt(pycurl.PROTOCOLS, pycurl.PROTO_HTTP | pycurl.PROTO_HTTPS)
            if hasattr(pycurl, "REDIR_PROTOCOLS"):
                curl.setopt(pycurl.REDIR_PROTOCOLS, pycurl.PROTO_HTTP | pycurl.PROTO_HTTPS)
            curl.setopt(pycurl.HTTPHEADER, self._build_headers(url, headers))
            curl.setopt(pycurl.HEADERFUNCTION, on_header)
            curl.setopt(pycurl.WRITEFUNCTION, on_write)
            curl.setopt(pycurl.ACCEPT_ENCODING, "")
            if options.http2:
                curl.setopt(pycurl.HTTP_VERSION, pycurl.CURL_HTTP_VERSION_2TLS)
            curl.setopt(pycurl.CONNECTTIMEOUT_MS, int(max(0.0, options.connect_timeout) * 1000))
            if options.timeout and options.timeout > 0:
                curl.setopt(pycurl.LOW_SPEED_LIMIT, 1)
                curl.setopt(pycurl.LOW_SPEED_TIME, int(options.timeout))
            if enforce_max_filesize and options.max_filesize:
                with suppress(Exception):
                    curl.setopt(pycurl.MAXFILESIZE, int(options.max_filesize))
            if options.insecure:
                curl.setopt(pycurl.SSL_VERIFYPEER, 0)
                curl.setopt(pycurl.SSL_VERIFYHOST, 0)
            if options.ca_cert:
                curl.setopt(pycurl.CAINFO, str(options.ca_cert))
            if options.client_cert:
                curl.setopt(pycurl.SSLCERT, str(options.client_cert))
            if options.client_key:
                curl.setopt(pycurl.SSLKEY, str(options.client_key))
            if options.proxy:
                curl.setopt(pycurl.PROXY, options.proxy)
            if options.no_proxy:
                curl.setopt(pycurl.PROXY, "")
                with suppress(Exception):
                    curl.setopt(pycurl.NOPROXY, "*")
            login, password = options.credentials_for(url)
            if login:
                curl.setopt(pycurl.USERPWD, f"{login}:{password or ''}")
            if effective_method == "HEAD":
                curl.setopt(pycurl.NOBODY, 1)
                curl.setopt(pycurl.CUSTOMREQUEST, "HEAD")
            elif options.data is not None:
                curl.setopt(pycurl.POSTFIELDS, options.data)
                if effective_method != "POST":
                    curl.setopt(pycurl.CUSTOMREQUEST, effective_method)
            elif effective_method != "GET":
                curl.setopt(pycurl.CUSTOMREQUEST, effective_method)
            if byte_range is not None:
                start, end = byte_range
                curl.setopt(pycurl.RANGE, f"{start}-{'' if end is None else end}")

            try:
                curl.perform()
            except pycurl.error as exc:
                if state["error"] is not None:
                    raise state["error"] from None
                if not state["stop"]:
                    raise _map_curl_error(exc, url, max_filesize=options.max_filesize) from exc

            status = int(curl.getinfo(pycurl.RESPONSE_CODE))
            final_url = str(curl.getinfo(pycurl.EFFECTIVE_URL) or url)
            parsed: dict[str, str] = {}
            set_cookies: list[str] = []
            for name, value in collected:
                key = name.lower()
                if key == "set-cookie":
                    set_cookies.append(value)
                elif key in parsed:
                    parsed[key] = f"{parsed[key]}, {value}"
                else:
                    parsed[key] = value
            response = Response(status=status, url=final_url, headers=parsed)
            if self.cookies is not None and set_cookies:
                self.cookies.update(final_url, set_cookies)
            if status >= 400 and status not in allow_status:
                raise HTTPStatusError(
                    f"HTTP {status} for {response.header('content-type', 'unknown content type')}",
                    status_code=status,
                    url=redact_url(final_url),
                    retryable=status in self.retry_status,
                )
            return response
        finally:
            curl.close()

    def probe(self, url: str) -> ProbeResult:
        info = self.request(url, method="HEAD", allow_status=frozenset({403, 405, 501}))
        size = info.int_header("content-length")
        accept_ranges = (info.header("accept-ranges") or "").lower() == "bytes"
        encoding = (info.header("content-encoding") or "identity").lower()
        if encoding not in {"", "identity"}:
            accept_ranges = False
        final_url = info.url
        if size is None or info.status in {403, 405, 501}:
            info = self.request(
                url,
                method="GET",
                headers=[("Range", "bytes=0-0")],
                sink=lambda _data: None,
                allow_status=frozenset({403, 405, 501}),
            )
            size = info.int_header("content-length")
            parsed = parse_content_range(info.header("content-range"))
            accept_ranges = info.status == 206
            if parsed is not None and parsed[2] is not None:
                size = parsed[2]
            final_url = info.url
        elif not accept_ranges and (size or 0) > 0:
            range_info = self.request(
                url,
                method="GET",
                headers=[("Range", "bytes=0-0")],
                sink=lambda _data: None,
            )
            if range_info.status == 206:
                accept_ranges = True
                parsed = parse_content_range(range_info.header("content-range"))
                if parsed is not None and parsed[2] is not None:
                    size = parsed[2]
        return ProbeResult(
            url=url,
            final_url=final_url,
            status=info.status,
            size=size,
            etag=info.header("etag"),
            last_modified=info.header("last-modified"),
            accept_ranges=accept_ranges,
            suggested_filename=content_disposition_filename(info.header("content-disposition")),
            headers=info.headers,
        )


class DiskSink:
    def __init__(
        self,
        fd: int,
        offset: int,
        *,
        state: PartState,
        progress: ProgressReporter,
        handle: int,
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
        with suppress(OSError):
            os.fsync(self.fd)
        self._state.save()
        self._unsynced = 0


class MemorySink:
    def __init__(
        self,
        *,
        progress: ProgressReporter,
        handle: int,
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

    def reset(self) -> None:
        self.buffer.clear()
        self.written = 0


class ProgressReporter:
    def __init__(self, mode: str = "rich", *, quiet: bool = False, overall: bool = False) -> None:
        self.mode = "none" if quiet else mode
        self._lock = threading.Lock()
        self._names: dict[int, str] = {}
        self._sizes: dict[int, int | None] = {}
        self._done: dict[int, int] = {}
        self._last_print: dict[int, float] = {}
        self._console = Console(stderr=True)
        self._rich: Progress | None = None
        self._rich_tasks: dict[int, TaskID] = {}
        self._rich_overall: TaskID | None = None
        self._counter = 0
        if self.mode == "rich":
            self._rich = Progress(
                TextColumn("[bold]{task.description}"),
                BarColumn(bar_width=24),
                TextColumn("{task.fields[status]}"),
                TransferSpeedColumn(),
                TimeElapsedColumn(),
                console=self._console,
                transient=False,
            )
            self._rich.start()
            if overall:
                self._rich_overall = self._rich.add_task("total", total=None, status="")

    def add_file(self, name: str, size: int | None, already: int = 0) -> int:
        with self._lock:
            handle = self._counter
            self._counter += 1
            self._names[handle] = name
            self._sizes[handle] = size
            self._done[handle] = already
            self._last_print[handle] = 0.0
        if self._rich is not None:
            with self._lock:
                task = self._rich.add_task(name, total=size, completed=already, status="")
                self._rich_tasks[handle] = task
        return handle

    def update(self, handle: int, advance: int) -> None:
        if self.mode == "none" or advance == 0:
            return
        with self._lock:
            self._done[handle] = max(0, self._done.get(handle, 0) + advance)
            done = self._done[handle]
        if self._rich is not None:
            with self._lock:
                task = self._rich_tasks.get(handle)
                if task is not None:
                    self._rich.update(task, advance=advance)
                    if self._rich_overall is not None:
                        self._rich.update(self._rich_overall, advance=advance)
            return
        self._print_plain(handle, done)

    def _print_plain(self, handle: int, done: int) -> None:
        now = time.monotonic()
        size = self._sizes.get(handle)
        with self._lock:
            last = self._last_print.get(handle, 0.0)
            if now - last < 0.2:
                return
            self._last_print[handle] = now
        name = self._names.get(handle, "")
        if size:
            percent = min(100.0, done * 100.0 / size)
            line = f"{name}: {percent:5.1f}% {human_size(done)}/{human_size(size)}"
        else:
            line = f"{name}: {human_size(done)}"
        print(f"\r{line}", end="", file=sys.stderr, flush=True)

    def finish(self, handle: int, status: str = "complete") -> None:
        if self.mode == "none":
            return
        name = self._names.get(handle, "")
        done = self._done.get(handle, 0)
        size = self._sizes.get(handle)
        if self._rich is not None:
            with self._lock:
                task = self._rich_tasks.get(handle)
                if task is not None:
                    self._rich.update(task, status=status, completed=size or done)
            return
        if not sys.stderr.isatty():
            print("", file=sys.stderr)
        label = f"{name}: {status}"
        if size:
            label += f" {human_size(size)}"
        print(f"\r{label}", file=sys.stderr, flush=True)

    def clear_line(self) -> None:
        if self.mode == "plain":
            print("\r" + " " * 78 + "\r", end="", file=sys.stderr, flush=True)

    def log(self, message: str) -> None:
        if self.mode == "rich":
            self._console.print(message)

    def close(self) -> None:
        if self._rich is not None:
            with self._lock:
                if self._rich_overall is not None:
                    self._rich.update(self._rich_overall, total=self._total(), completed=self._total())
                self._rich.stop()
            self._rich = None

    def _total(self) -> int:
        return sum(value or 0 for value in self._done.values())


@dataclass(frozen=True)
class OutputOptions:
    directory: Path = field(default_factory=Path)
    output: Path | None = None
    template: str | None = None
    no_clobber: bool = False
    overwrite: bool = True
    timestamping: bool = False
    unique: bool = False
    manifest: bool = False
    content_disposition: bool = True


@dataclass(frozen=True)
class Settings:
    connections: int = 8
    max_per_host: int = 6
    jobs: int = 1
    max_active: int = 1
    retry: int = 5
    retry_delay: float = 1.0
    retry_backoff: float = 2.0
    retry_max_delay: float = 60.0
    retry_jitter: float = 0.25
    retry_http: frozenset[int] = DEFAULT_RETRY_STATUS
    continue_: bool = False
    allow_unvalidated_resume: bool = False
    keep_partial: bool = True
    remove_on_error: bool = False
    check_disk: bool = True
    checksums: tuple[ChecksumSpec, ...] = ()
    checksum_table: Mapping[str, Sequence[ChecksumSpec]] = field(default_factory=dict)
    request: RequestOptions = field(default_factory=RequestOptions)
    output: OutputOptions = field(default_factory=OutputOptions)
    max_filesize: int | None = None
    rate_limit: float | None = None
    rate_limit_per_connection: float | None = None
    buffer_threshold: int = CHUNK_THRESHOLD
    dry_run: bool = False
    spider: bool = False
    recursive: bool = False
    mirror: bool = False
    mirrors: tuple[str, ...] = ()
    accept: tuple[str, ...] = ()
    reject: tuple[str, ...] = ()
    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()
    allow_unsafe_schemes: bool = False

    def retry_policy(self) -> RetryPolicy:
        return RetryPolicy(
            max_retries=self.retry,
            delay=self.retry_delay,
            backoff=self.retry_backoff,
            max_delay=self.retry_max_delay,
            jitter=self.retry_jitter,
            retry_status=self.retry_http,
        )

    def effective_connections(self, size: int | None) -> int:
        configured = max(1, min(self.connections, MAX_CONNECTIONS_PER_FILE))
        if not size or size < MULTI_RANGE_MIN:
            return 1
        return max(1, min(configured, max(1, size // (256 * 1024))))

    def global_limiter(self) -> TokenBucket | None:
        return TokenBucket(self.rate_limit) if self.rate_limit else None

    def connection_limiter(self) -> TokenBucket | None:
        return TokenBucket(self.rate_limit_per_connection) if self.rate_limit_per_connection else None

    def specs_for(self, filename: str) -> list[ChecksumSpec]:
        specs = list(self.checksums)
        specs.extend(self.checksum_table.get(filename, ()))
        return specs


@dataclass(frozen=True)
class Resolution:
    path: Path
    skip_reason: str | None = None
    detail: str | None = None

    @property
    def skipped(self) -> bool:
        return self.skip_reason is not None


class _SafeDict(dict[str, Any]):
    def __missing__(self, key: str) -> str:
        return ""


def validate_template(template: str) -> None:
    for _, field_name, _, _ in string.Formatter().parse(template):
        if not field_name:
            continue
        base = re.split(r"[.\[{]", field_name, maxsplit=1)[0]
        if base and base not in TEMPLATE_FIELDS:
            raise UsageError(
                f"unknown field '{base}' in --output-template " f"(available: {', '.join(sorted(TEMPLATE_FIELDS))})"
            )


def render_template(template: str, values: Mapping[str, Any]) -> str:
    try:
        return string.Formatter().vformat(template, (), _SafeDict(values))
    except (ValueError, IndexError) as exc:
        raise UsageError(f"invalid --output-template {template!r}: {exc}") from exc


def template_values(url: str, filename: str, index: int, size: int | None) -> dict[str, Any]:
    stem, dot, ext = filename.rpartition(".")
    if not dot:
        stem, ext = filename, ""
    parsed = urlparse(url)
    return {
        "url": url,
        "host": url_host(url) or "download",
        "path": parsed.path.lstrip("/") or filename,
        "name": stem,
        "filename": filename,
        "stem": stem,
        "ext": ext,
        "index": index,
        "size": size if size is not None else "",
        "date": time.strftime("%Y-%m-%d"),
    }


def resolve_output(options: OutputOptions, probe: ProbeResult, url: str, *, index: int = 0) -> Resolution:
    filename = filename_from_url(url)
    if options.content_disposition:
        from_header = content_disposition_filename(probe.header("content-disposition"))
        if from_header:
            filename = from_header
    elif probe.suggested_filename:
        filename = sanitize_filename(probe.suggested_filename)
    if options.template:
        validate_template(options.template)
        rendered = render_template(options.template, template_values(url, filename, index, probe.size))
        rendered = rendered.replace("\\", "/").lstrip("/")
        raw_parts = [part for part in rendered.split("/") if part not in {"", "."}]
        if any(part == ".." for part in raw_parts):
            raise UsageError(f"--output-template must not contain '..': {options.template!r}")
        parts = [sanitize_filename(part) for part in raw_parts]
        if not parts:
            raise UsageError(f"--output-template produced an empty path: {options.template!r}")
        candidate = options.directory.joinpath(*parts)
    elif options.output is not None:
        candidate = options.output
        if not candidate.is_absolute():
            candidate = options.directory / candidate
    else:
        candidate = options.directory / filename
    if options.no_clobber and candidate.exists():
        return Resolution(candidate, "skipped", "exists (--no-clobber)")
    if options.timestamping and candidate.exists():
        remote_ts = probe.last_modified_ts
        try:
            local_ts = candidate.stat().st_mtime
        except OSError:
            local_ts = 0.0
        if remote_ts is not None and local_ts + 1 >= remote_ts:
            return Resolution(candidate, "up-to-date", "local copy is newer (--timestamping)")
    if options.unique and candidate.exists():
        candidate = unique_path(candidate)
    return Resolution(candidate)


def write_manifest(
    final_path: Path,
    *,
    url: str,
    final_url: str | None = None,
    etag: str | None = None,
    last_modified: str | None = None,
    size: int | None = None,
    digests: Mapping[str, str] | None = None,
) -> Path:
    payload: dict[str, Any] = {
        "manifest_version": 1,
        "tool": f"wget2-single {__version__}",
        "url": redact_url(url),
        "final_url": redact_url(final_url or url),
        "filename": final_path.name,
        "path": str(final_path),
        "size": size if size is not None else final_path.stat().st_size,
        "etag": etag,
        "last_modified": last_modified,
        "checksums": dict(digests or {}),
        "downloaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "mtime": final_path.stat().st_mtime,
    }
    manifest_path = final_path.with_name(final_path.name + MANIFEST_SUFFIX)
    try:
        manifest_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    except OSError as exc:
        raise FilesystemError(f"cannot write manifest {manifest_path}: {exc}") from exc
    return manifest_path


class HostLimiter:
    def __init__(self, limit: int) -> None:
        self.limit = max(1, limit)
        self._semaphores: dict[str, threading.Semaphore] = {}
        self._lock = threading.Lock()

    def _semaphore(self, url: str) -> threading.Semaphore:
        host = (url_host(url) or "").lower()
        with self._lock:
            semaphore = self._semaphores.get(host)
            if semaphore is None:
                semaphore = threading.Semaphore(self.limit)
                self._semaphores[host] = semaphore
            return semaphore

    @contextmanager
    def slot(self, url: str) -> Iterator[None]:
        semaphore = self._semaphore(url)
        semaphore.acquire()
        try:
            yield
        finally:
            semaphore.release()


class PathLocks:
    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()

    def _lock_for(self, path: Path) -> threading.Lock:
        try:
            key = str(path.resolve())
        except OSError:
            key = str(path)
        with self._guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock
            return lock

    @contextmanager
    def hold(self, path: Path) -> Iterator[None]:
        lock = self._lock_for(path)
        lock.acquire()
        try:
            yield
        finally:
            lock.release()


@dataclass
class Result:
    url: str
    path: Path | None = None
    status: str = "pending"
    error: str | None = None
    error_kind: str | None = None
    exit_code: int = EXIT_OK
    size: int | None = None
    bytes_written: int = 0
    duration: float = 0.0
    resumed: bool = False
    connections: int = 1
    attempts: int = 1
    etag: str | None = None
    checksums: dict[str, str] = field(default_factory=dict)
    detail: str | None = None

    @property
    def ok(self) -> bool:
        return self.status in {"downloaded", "skipped", "up-to-date", "spidered", "dry-run"}

    def to_dict(self) -> dict[str, Any]:
        return {
            "url": redact_url(self.url),
            "path": str(self.path) if self.path else None,
            "status": self.status,
            "error": self.error,
            "error_kind": self.error_kind,
            "exit_code": self.exit_code,
            "size": self.size,
            "bytes_written": self.bytes_written,
            "duration": round(self.duration, 3),
            "resumed": self.resumed,
            "connections": self.connections,
            "attempts": self.attempts,
            "etag": self.etag,
            "checksums": self.checksums,
            "detail": self.detail,
        }


class Downloader:
    def __init__(
        self,
        settings: Settings,
        log: Logger,
        progress: ProgressReporter,
        interrupt: InterruptFlag,
        client_factory: Callable[[], HttpClient],
        host_limiter: HostLimiter,
        path_locks: PathLocks,
        global_limiter: TokenBucket | None,
    ) -> None:
        self.settings = settings
        self.log = log
        self.progress = progress
        self.interrupt = interrupt
        self.client_factory = client_factory
        self.host_limiter = host_limiter
        self.path_locks = path_locks
        self.global_limiter = global_limiter

    def _limiters(self, extra: TokenBucket | None) -> MultiLimiter:
        limiters: list[TokenBucket] = []
        if self.global_limiter is not None:
            limiters.append(self.global_limiter)
        if extra is not None:
            limiters.append(extra)
        return MultiLimiter(limiters)

    def _sleep(self, delay: float) -> None:
        deadline = time.monotonic() + max(0.0, delay)
        while time.monotonic() < deadline:
            self.interrupt.check()
            time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))

    def run(self, url: str, index: int = 0) -> Result:
        started = time.monotonic()
        mirrors = [item for item in self.settings.mirrors if item and item != url]
        candidates = [url, *mirrors]
        result = Result(url=url)
        for attempt, candidate in enumerate(candidates, start=1):
            result = Result(url=url)
            self._attempt(candidate, url, index, result)
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

    def _attempt(self, candidate: str, name_url: str, index: int, result: Result) -> None:
        started = time.monotonic()
        try:
            url = validate_url(candidate, allow_extra_schemes=self.settings.allow_unsafe_schemes)
            client = self.client_factory()
            policy = self.settings.retry_policy()
            attempt = 1
            while True:
                try:
                    probe = client.probe(url)
                    break
                except Wget2Error as exc:
                    if not policy.should_retry(exc) or attempt >= policy.attempts:
                        raise
                    delay = policy.delay_for(attempt)
                    self.log.warning(
                        "retrying probe",
                        url=redact_url(url),
                        attempt=attempt,
                        error=str(exc),
                        delay=f"{delay:.2f}s",
                    )
                    self._sleep(delay)
                    attempt += 1
            result.etag = probe.etag
            result.size = probe.size
            self._execute(url, index, probe, result, started, name_url)
        except Interrupted as exc:
            result.status = "failed"
            result.error = str(exc)
            result.error_kind = "interrupted"
            result.exit_code = EXIT_INTERRUPTED
        except Wget2Error as exc:
            result.status = "failed"
            if self.interrupt.is_set():
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
        except OSError as exc:
            result.status = "failed"
            result.error = f"{exc.__class__.__name__}: {exc}"
            result.error_kind = "filesystem"
            result.exit_code = EXIT_FILESYSTEM
        except Exception as exc:
            result.status = "failed"
            result.error = f"{exc.__class__.__name__}: {exc}"
            result.error_kind = "internal"
            result.exit_code = EXIT_INTERNAL

    def _execute(self, url: str, index: int, probe: ProbeResult, result: Result, started: float, name_url: str) -> None:
        settings = self.settings
        size = probe.size
        if settings.max_filesize and size is not None and size > settings.max_filesize:
            raise PolicyError(
                f"resource is {human_size(size)}, above --max-filesize ({human_size(settings.max_filesize)})",
                url=redact_url(url),
            )
        if size == 0:
            raise PolicyError(f"refusing to download empty resource: {redact_url(url)}")
        if settings.spider or settings.dry_run:
            planned = resolve_output(settings.output, probe, name_url, index=index)
            result.path = planned.path
            result.status = "spidered" if settings.spider else "dry-run"
            result.size = size
            result.detail = (
                f"{'spider' if settings.spider else 'dry-run'} {human_size(size)} -> "
                f"{planned.path} ({'ranges' if probe.accept_ranges else 'single stream'})"
            )
            self.log.info(
                "probe complete",
                url=redact_url(url),
                path=str(planned.path),
                size=size,
                ranges=probe.accept_ranges,
            )
            self.progress.log(result.detail)
            return
        if settings.check_disk and size:
            ensure_free_space(resolve_output(settings.output, probe, name_url, index=index).path.parent, size)
        initial = resolve_output(settings.output, probe, name_url, index=index)
        with self.path_locks.hold(initial.path):
            resolution = resolve_output(settings.output, probe, name_url, index=index)
            result.path = resolution.path
            if resolution.skipped:
                result.status = resolution.skip_reason or "skipped"
                result.detail = resolution.detail
                result.exit_code = EXIT_OK
                self.log.info(
                    "skipping file",
                    url=redact_url(url),
                    path=str(resolution.path),
                    reason=resolution.detail,
                )
                return
            self._transfer(url, probe, result, started, resolution.path, size)

    def _transfer(
        self,
        url: str,
        probe: ProbeResult,
        result: Result,
        started: float,
        final_path: Path,
        size: int | None,
    ) -> None:
        settings = self.settings
        client = self.client_factory()
        ensure_directory(final_path.parent)
        specs = settings.specs_for(final_path.name)
        connections = settings.effective_connections(size)
        use_ranges = bool(
            probe.accept_ranges
            and size
            and size >= MULTI_RANGE_MIN
            and connections > 1
            and (settings.request.method or "GET").upper() == "GET"
            and settings.request.data is None
        )
        if use_ranges and size:
            count = min(
                max(connections, connections * SEGMENTS_PER_CONNECTION),
                max(1, size // (256 * 1024)),
            )
            plan = plan_ranges(size, max(1, count))
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
            resume=settings.continue_,
            allow_unvalidated=settings.allow_unvalidated_resume,
            log=self.log,
        )
        result.resumed = resumed
        if resumed and not probe.accept_ranges and state.done_bytes():
            self.log.warning("server no longer supports ranges; restarting download", url=redact_url(url))
            state.covered = []
            state.plan = [(0, size)] if size else []
            resumed = False
            result.resumed = False
        buffered = bool(size is not None and size <= settings.buffer_threshold)
        self.log.info(
            "starting download",
            url=redact_url(url),
            path=str(final_path),
            size=size,
            connections=connections if use_ranges else 1,
            mode="buffered" if buffered else "chunked→disk",
            resumed=resumed,
            done_bytes=state.done_bytes() if resumed else 0,
            etag=probe.etag,
        )
        result.connections = (min(connections, len(plan)) if use_ranges else 1) or 1
        handle = self.progress.add_file(final_path.name, size, already=state.done_bytes() if resumed else 0)
        status = "failed"
        failed = False
        failure: BaseException | None = None
        fd = -1
        try:
            if not resumed or not part_path.exists():
                preallocate(part_path, size or 0)
            elif size and part_path.stat().st_size != size:
                preallocate(part_path, size)
            try:
                fd = os.open(str(part_path), os.O_WRONLY)
            except OSError as exc:
                raise FilesystemError(f"cannot open {part_path} for writing: {exc}") from exc
            bytes_before = state.done_bytes()
            if size is None:
                self._download_single(client, url, state, size, fd, handle)
            else:
                missing = state.missing() if resumed else plan
                missing = [segment for segment in missing if segment[1] > segment[0]]
                if not missing:
                    self.log.info("partial file already complete", path=str(part_path))
                elif use_ranges and len(missing) > 1:
                    try:
                        self._download_segments(client, url, state, missing, size, fd, handle, buffered, connections)
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
                        self._download_single(client, url, state, size, fd, handle)
                else:
                    self._download_single(client, url, state, size, fd, handle)
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
            if settings.output.manifest:
                write_manifest(
                    final_path,
                    url=url,
                    final_url=probe.final_url,
                    etag=probe.etag,
                    last_modified=probe.last_modified,
                    size=result.size,
                    digests=digests,
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
        except BaseException as exc:
            failed = True
            failure = exc
            raise
        finally:
            if fd >= 0:
                with suppress(OSError):
                    os.close(fd)
            if failed:
                self._cleanup_failure(state, part_path, failure)
            self.progress.finish(handle, status=status)

    def _cleanup_failure(self, state: PartState, part_path: Path, failure: BaseException | None) -> None:
        if isinstance(failure, (ChecksumError, IntegrityError)):
            safe_unlink(part_path)
            state.discard()
            self.log.warning("discarded corrupt partial data", path=str(part_path))
            return
        if self.settings.remove_on_error:
            safe_unlink(part_path)
            state.discard()
            return
        if state.done_bytes() > 0:
            with suppress(Wget2Error, OSError):
                state.save()
            self.log.info(
                "kept partial file for resume",
                path=str(part_path),
                hint="use --continue/-C to resume",
            )

    def _download_segments(
        self,
        client: HttpClient,
        url: str,
        state: PartState,
        segments: Sequence[Interval],
        size: int | None,
        fd: int,
        handle: int,
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
            limiter = self.settings.connection_limiter()
            while not stop.is_set():
                try:
                    segment = queue.get_nowait()
                except Empty:
                    return
                try:
                    self._fetch_segment(client, url, state, segment, size, fd, handle, buffered, limiter)
                except BaseException as exc:
                    stop.set()
                    errors.append(exc)
                    return
                finally:
                    queue.task_done()

        threads = [
            threading.Thread(target=worker, name=f"wget2-range-{index}", daemon=True) for index in range(workers)
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
        client: HttpClient,
        url: str,
        state: PartState,
        segment: Interval,
        size: int | None,
        fd: int,
        handle: int,
        buffered: bool,
        connection_limiter: TokenBucket | None,
    ) -> None:
        start, end = segment
        policy = self.settings.retry_policy()
        attempt = 1
        while True:
            self.interrupt.check()
            offset = start if buffered else state.resume_offset(start, end)
            if offset >= end:
                return
            limiters = self._limiters(connection_limiter)
            sink: MemorySink | DiskSink
            if buffered:
                sink = MemorySink(
                    progress=self.progress,
                    handle=handle,
                    limiters=limiters,
                    interrupt=self.interrupt,
                )
            else:
                sink = DiskSink(
                    fd,
                    offset,
                    state=state,
                    progress=self.progress,
                    handle=handle,
                    limiters=limiters,
                    interrupt=self.interrupt,
                )
            try:
                with self.host_limiter.slot(url):
                    info = client.request(
                        url,
                        sink=sink.feed,
                        byte_range=(offset, end - 1),
                        enforce_max_filesize=True,
                    )
                self._check_ranged_response(info, offset, end, size)
                expected = end - offset
                if sink.written < expected:
                    raise IntegrityError(
                        f"short read for bytes {offset}-{end - 1}: " f"expected {expected} bytes, got {sink.written}",
                        url=redact_url(url),
                    )
                if buffered:
                    assert isinstance(sink, MemorySink)
                    self._commit_memory(fd, state, sink, offset)
                return
            except Wget2Error as exc:
                if buffered:
                    assert isinstance(sink, MemorySink)
                    if sink.written:
                        self.progress.update(handle, advance=-sink.written)
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
        client: HttpClient,
        url: str,
        state: PartState,
        size: int | None,
        fd: int,
        handle: int,
    ) -> None:
        policy = self.settings.retry_policy()
        attempt = 1
        restarted = False
        connection_limiter = self.settings.connection_limiter()
        while True:
            self.interrupt.check()
            offset = state.resume_offset(0, size) if size else 0
            if size and offset >= size:
                return
            sink = DiskSink(
                fd,
                offset,
                state=state,
                progress=self.progress,
                handle=handle,
                limiters=self._limiters(connection_limiter),
                interrupt=self.interrupt,
            )
            try:
                with self.host_limiter.slot(url):
                    info = client.request(
                        url,
                        sink=sink.feed,
                        byte_range=(offset, None) if offset else None,
                        enforce_max_filesize=True,
                    )
                if info.status == 200:
                    declared = info.int_header("content-length")
                    if size is None and declared is not None:
                        size = declared
                        state.size = size
                        state.plan = [(0, size)]
                    if size is not None and sink.written < size - offset:
                        raise IntegrityError(
                            f"incomplete body: expected {size - offset} bytes, got {sink.written}",
                            url=redact_url(url),
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

    def _check_ranged_response(self, info: Response, start: int, end: int, size: int | None) -> None:
        if info.status == 200:
            if start > 0 or (size is not None and end < size):
                raise RangeNotSupportedError("server ignored the Range header (answered 200)", url=redact_url(info.url))
            return
        if info.status != 206:
            return
        parsed = parse_content_range(info.header("content-range"))
        if not parsed:
            raise IntegrityError("206 response without a valid Content-Range header", url=redact_url(info.url))
        range_start, _range_end, total = parsed
        if range_start != start:
            raise IntegrityError(
                f"server returned range starting at {range_start}, expected {start}",
                url=redact_url(info.url),
            )
        if total is not None and size is not None and total != size:
            raise IntegrityError(
                f"resource size changed during download ({size} -> {total})",
                url=redact_url(info.url),
            )

    def _verify_complete(self, state: PartState, size: int | None, part_path: Path) -> None:
        if size is None:
            return
        done = state.done_bytes()
        if done < size:
            raise IntegrityError(f"incomplete download: {done} of {size} bytes", url=redact_url(state.url))
        try:
            actual = part_path.stat().st_size
        except OSError as exc:
            raise FilesystemError(f"cannot stat {part_path}: {exc}") from exc
        if actual != size:
            raise IntegrityError(
                f"size mismatch for {part_path.name}: expected {size}, got {actual}",
                url=redact_url(state.url),
            )


class Crawler:
    def __init__(
        self,
        settings: Settings,
        log: Logger,
        client_factory: Callable[[], HttpClient],
        level: int,
    ) -> None:
        self.settings = settings
        self.log = log
        self.client_factory = client_factory
        self.level = level

    def _allowed(self, url: str, root_host: str | None) -> bool:
        if (urlparse(url).scheme or "").lower() not in ALLOWED_SCHEMES:
            return False
        return not (root_host and (url_host(url) or "") != root_host)

    def _selected(self, url: str) -> bool:
        name = filename_from_url(url)
        if self.settings.accept and not any(fnmatch(name, pattern) for pattern in self.settings.accept):
            return False
        if self.settings.reject and any(fnmatch(name, pattern) for pattern in self.settings.reject):
            return False
        if self.settings.include and not any(fnmatch(url, pattern) for pattern in self.settings.include):
            return False
        return not (self.settings.exclude and any(fnmatch(url, pattern) for pattern in self.settings.exclude))

    def discover(self, seeds: Sequence[str]) -> list[str]:
        if self.settings.mirror:
            self.level = max(self.level, 5)

        found: list[str] = []
        seen: set[str] = set()
        queue: Queue[tuple[str, int]] = Queue()
        root_host = url_host(seeds[0]) if seeds else None
        for seed in seeds:
            if seed not in seen:
                seen.add(seed)
                queue.put((seed, 0))
        client = self.client_factory()
        while not queue.empty():
            url, depth = queue.get_nowait()
            if self._selected(url):
                found.append(url)
            if depth >= self.level:
                continue
            buffer = bytearray()

            def collect(chunk: bytes, target: bytearray = buffer) -> None:
                if len(target) < 4 * 1024 * 1024:
                    target.extend(chunk)

            try:
                response = client.request(
                    url,
                    method="GET",
                    headers=[("Range", "bytes=0-1048575")],
                    sink=collect,
                    byte_range=(0, 1048575),
                )
            except Wget2Error as exc:
                self.log.debug("crawl fetch failed", url=redact_url(url), error=str(exc))
                continue
            content_type = (response.header("content-type") or "").lower()
            if "html" not in content_type:
                continue
            text = buffer.decode("utf-8", errors="replace")
            for match in _LINK_RE.finditer(text):
                raw = next(group for group in match.groups() if group)
                if raw.startswith(("mailto:", "javascript:", "#", "data:")):
                    continue
                absolute = urljoin(url, raw.strip()).split("#", 1)[0]
                if absolute in seen or not self._allowed(absolute, root_host):
                    continue
                seen.add(absolute)
                queue.put((absolute, depth + 1))
        return found


class Runner:
    def __init__(
        self,
        settings: Settings,
        log: Logger,
        progress: ProgressReporter,
        interrupt: InterruptFlag,
    ) -> None:
        self.settings = settings
        self.log = log
        self.progress = progress
        self.interrupt = interrupt
        self.cookies = CookieJar()

    def _client(self) -> HttpClient:
        return HttpClient(self.settings.request, self.log, self.cookies, self.settings.retry_http)

    def run(self, urls: Sequence[str]) -> list[Result]:
        settings = self.settings
        workers = max(1, min(len(urls), settings.max_active, max(1, settings.jobs)))
        self.log.info(
            "batch started",
            urls=len(urls),
            workers=workers,
            per_file_connections=settings.connections,
        )
        downloader = Downloader(
            settings,
            self.log,
            self.progress,
            self.interrupt,
            self._client,
            HostLimiter(settings.max_per_host),
            PathLocks(),
            settings.global_limiter(),
        )
        results: list[Result | None] = [None] * len(urls)

        def work(index: int, url: str) -> None:
            results[index] = downloader.run(url, index)

        if workers == 1:
            for index, url in enumerate(urls):
                work(index, url)
        else:
            queue: Queue[tuple[int, str]] = Queue()
            for index, url in enumerate(urls):
                queue.put((index, url))

            def worker() -> None:
                while True:
                    try:
                        index, url = queue.get_nowait()
                    except Empty:
                        return
                    try:
                        work(index, url)
                    finally:
                        queue.task_done()

            pool = [
                threading.Thread(target=worker, name=f"wget2-file-{number}", daemon=True) for number in range(workers)
            ]
            for thread in pool:
                thread.start()
            for thread in pool:
                while thread.is_alive():
                    thread.join(timeout=0.2)
        return [item for item in results if item is not None]


_CONFIG_PATHS: tuple[Path, ...] = (Path("wget2.toml"), Path.home() / ".config" / "wget2" / "wget2.toml")
_CLI_LISTS = frozenset({"header", "checksum", "accept", "reject", "include", "exclude", "mirror_list"})


def _config_from_argv(argv: Sequence[str]) -> Path | None:
    for index, item in enumerate(argv):
        if item == "--no-config":
            return None
        if item == "--config" and index + 1 < len(argv):
            return Path(argv[index + 1])
        if item.startswith("--config="):
            return Path(item.split("=", 1)[1])
    for candidate in _CONFIG_PATHS:
        if candidate.exists():
            return candidate
    return None


def _load_config(argv: Sequence[str]) -> dict[str, Any]:
    path = _config_from_argv(argv)
    data: dict[str, Any] = {}
    if path is not None:
        if not path.exists():
            print(f"config file not found: {path}", file=sys.stderr)
            return {}
        try:
            import tomllib

            data = dict(tomllib.loads(path.read_text(encoding="utf-8")))
        except (OSError, ValueError) as exc:
            print(f"cannot read {path}: {exc}", file=sys.stderr)
            return {}
        flat: dict[str, Any] = {}
        for key, value in data.items():
            if isinstance(value, dict):
                flat.update(value)
            else:
                flat[key] = value
        data = flat
    for key, value in os.environ.items():
        if not key.startswith("WGET2_"):
            continue
        name = key[len("WGET2_") :].lower()
        if name in {"checksum", "header", "accept", "reject", "include", "exclude", "mirror_list"}:
            data[name] = [item for item in value.split(",") if item]
        elif name in {"jobs", "retry", "connections", "max_active", "max_per_host", "level"}:
            with suppress(ValueError):
                data[name] = int(value)
        else:
            data[name] = value
    return data


_CONFIG: dict[str, Any] = _load_config(sys.argv[1:])


def _d(key: str, default: Any) -> Any:
    value = _CONFIG.get(key)
    return default if value is None else value


def _install_interrupt(flag: InterruptFlag) -> Callable[[], None]:
    def handler(_signum: int, _frame: Any) -> None:
        count = flag.set()
        if count >= 2:
            raise KeyboardInterrupt
        print(
            "\ninterrupt received: stopping safely (press Ctrl+C again to force quit)",
            file=sys.stderr,
            flush=True,
        )

    previous = signal.signal(signal.SIGINT, handler)

    def restore() -> None:
        signal.signal(signal.SIGINT, previous)

    return restore


def _read_input(sources: Sequence[str], input_format: str, base_url: str | None) -> list[str]:
    urls: list[str] = []
    for source in sources:
        if source == "-":
            text = sys.stdin.read()
        else:
            path = Path(source)
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except OSError as exc:
                raise ConfigError(f"cannot read input file {source}: {exc}") from exc
        fmt = input_format
        if fmt == "auto":
            stripped = text.lstrip()
            if stripped.startswith(("[", "{")):
                fmt = "json"
            elif "," in text.splitlines()[0] if text.splitlines() else False:
                fmt = "csv"
            else:
                fmt = "plain"
        if fmt == "json":
            try:
                payload = json.loads(text)
            except ValueError as exc:
                raise ConfigError(f"invalid JSON input: {exc}") from exc
            items: Any = payload.get("urls") or payload.get("files") or [] if isinstance(payload, dict) else payload
            for item in items or ():
                if isinstance(item, str):
                    urls.append(item)
                elif isinstance(item, Mapping):
                    for key in ("url", "href", "link"):
                        value = item.get(key)
                        if isinstance(value, str):
                            urls.append(value)
                            break
        elif fmt == "csv":
            reader = csv.DictReader(io.StringIO(text))
            if reader.fieldnames and any(name.strip().lower() in {"url", "href", "link"} for name in reader.fieldnames):
                key = next(name for name in reader.fieldnames if name.strip().lower() in {"url", "href", "link"})
                urls.extend(row[key].strip() for row in reader if row.get(key, "").strip())
            else:
                for row in csv.reader(io.StringIO(text)):
                    if row and row[0].strip():
                        urls.append(row[0].strip())
        else:
            for line in text.splitlines():
                stripped = line.strip()
                if stripped and not stripped.startswith("#"):
                    urls.append(stripped)
    resolved: list[str] = []
    for url in urls:
        if base_url and not urlparse(url).scheme:
            resolved.append(urljoin(base_url, url))
        else:
            resolved.append(url)
    return resolved


def _parse_method_data(method: str | None, data: str | None) -> tuple[str, bytes | None]:
    payload: bytes | None = None
    if data is not None:
        if data.startswith("@") and len(data) > 1:
            path = Path(data[1:])
            try:
                payload = path.read_bytes()
            except OSError as exc:
                raise ConfigError(f"cannot read data file {path}: {exc}") from exc
        else:
            payload = data.encode()
    effective = (method or ("POST" if payload is not None else "GET")).upper()
    return effective, payload


def _build_settings(options: Mapping[str, Any]) -> Settings:
    output = OutputOptions(
        directory=Path(options["output_dir"]),
        output=Path(options["output"]) if options["output"] else None,
        template=options["output_template"],
        no_clobber=bool(options["no_clobber"]),
        overwrite=bool(options["overwrite"]),
        timestamping=bool(options["timestamping"]),
        unique=bool(options["unique"]),
        manifest=bool(options["manifest"]),
        content_disposition=bool(options["content_disposition"]),
    )
    method, payload = _parse_method_data(options["method"], options["data"])
    custom_headers: list[tuple[str, str]] = []
    for raw in options["header"] or ():
        if ":" in str(raw):
            name, _, value = str(raw).partition(":")
            custom_headers.append((name.strip(), value.strip()))
    request = RequestOptions(
        timeout=float(options["timeout"]),
        connect_timeout=float(options["connect_timeout"]),
        max_redirects=int(options["max_redirects"]),
        insecure=bool(options["insecure"]),
        ca_cert=Path(options["ca_cert"]) if options["ca_cert"] else None,
        client_cert=Path(options["client_cert"]) if options["client_cert"] else None,
        client_key=Path(options["client_key"]) if options["client_key"] else None,
        user_agent=options["user_agent"],
        referer=options["referer"],
        headers=tuple(custom_headers),
        user=options["user"],
        password=options["password"],
        bearer_token=options["bearer_token"],
        netrc=Path(options["netrc"]) if options["netrc"] else None,
        method=method,
        data=payload,
        proxy=options["proxy"],
        no_proxy=bool(options["no_proxy"]),
        http2=not bool(options["http1"]),
        max_filesize=parse_size(options["max_filesize"]) if options["max_filesize"] else None,
    )
    checksum_table: dict[str, list[ChecksumSpec]] = {}
    if options["checksum_file"]:
        checksum_table = load_checksum_file(Path(options["checksum_file"]))
    retry_http = parse_int_list(options["retry_http"]) if options["retry_http"] else DEFAULT_RETRY_STATUS
    return Settings(
        connections=int(options["connections"]),
        max_per_host=int(options["max_per_host"]),
        jobs=int(options["jobs"]),
        max_active=int(options["max_active"]),
        retry=int(options["retry"]),
        retry_delay=float(options["retry_delay"]),
        retry_backoff=float(options["retry_backoff"]),
        retry_max_delay=float(options["retry_max_delay"]),
        retry_jitter=float(options["retry_jitter"]),
        retry_http=retry_http,
        continue_=bool(options["continue_"]),
        allow_unvalidated_resume=bool(options["allow_unvalidated_resume"]),
        keep_partial=bool(options["keep_partial"]),
        remove_on_error=bool(options["remove_on_error"]),
        check_disk=bool(options["check_disk"]),
        checksums=tuple(parse_checksums(options["checksum"])),
        checksum_table=checksum_table,
        request=request,
        output=output,
        max_filesize=parse_size(options["max_filesize"]) if options["max_filesize"] else None,
        rate_limit=parse_rate(options["rate_limit"]),
        rate_limit_per_connection=parse_rate(options["rate_limit_per_connection"]),
        buffer_threshold=parse_size(options["buffer_threshold"]) or CHUNK_THRESHOLD,
        dry_run=bool(options["dry_run"]),
        spider=bool(options["spider"]),
        recursive=bool(options["recursive"]),
        mirror=bool(options["mirror"]),
        mirrors=tuple(options["mirror_list"] or ()),
        accept=tuple(options["accept"] or ()),
        reject=tuple(options["reject"] or ()),
        include=tuple(options["include"] or ()),
        exclude=tuple(options["exclude"] or ()),
        allow_unsafe_schemes=bool(options["allow_unsafe_schemes"]),
    )


def _progress_mode(progress: str, quiet: bool, verbose: bool, json_report: bool, urls: int) -> str:
    if json_report or quiet:
        return "none"
    if progress == "none":
        return "none"
    return progress


def _summary_exit(results: Sequence[Result], interrupted: bool) -> int:
    if interrupted:
        return EXIT_INTERRUPTED
    failed = [result for result in results if result.status == "failed"]
    if not failed:
        return EXIT_OK
    if len(results) == 1:
        return failed[0].exit_code
    if len(failed) == len(results):
        return failed[0].exit_code
    return EXIT_BATCH


def _print_summary(results: Sequence[Result], elapsed: float, console: Console) -> None:
    marks = {
        "downloaded": ("✔", "green"),
        "complete": ("✔", "green"),
        "skipped": ("=", "yellow"),
        "up-to-date": ("=", "yellow"),
        "spidered": ("·", "cyan"),
        "dry-run": ("·", "cyan"),
        "failed": ("✖", "red"),
    }
    for result in results:
        mark, colour = marks.get(result.status, ("·", "white"))
        name = result.path.name if result.path else filename_from_url(result.url)
        size = human_size(result.size) if result.size else ""
        line = f"[{colour}]{mark}[/{colour}] {name:<42} {size:>10}"
        if result.status == "failed" and result.error:
            line += f"  [{result.error_kind}] {result.error}"
        elif result.detail:
            line += f"  ({result.detail})"
        console.print(line, soft_wrap=True)
    total = sum(result.size or 0 for result in results if result.status == "downloaded")
    downloaded = sum(1 for result in results if result.status == "downloaded")
    skipped = sum(1 for result in results if result.status in {"skipped", "up-to-date"})
    checked = sum(1 for result in results if result.status in {"spidered", "dry-run"})
    failed = sum(1 for result in results if result.status == "failed")
    line = f"— {downloaded} downloaded, {skipped} skipped, {failed} failed"
    if checked:
        line += f", {checked} checked"
    console.print(f"{line}, {human_size(total)} in {format_duration(elapsed)}", soft_wrap=True)


def _report(results: Sequence[Result], elapsed: float, exit_code: int) -> dict[str, Any]:
    return {
        "tool": f"wget2-single {__version__}",
        "summary": {
            "total": len(results),
            "downloaded": sum(1 for item in results if item.status == "downloaded"),
            "skipped": sum(1 for item in results if item.status in {"skipped", "up-to-date"}),
            "failed": sum(1 for item in results if item.status == "failed"),
            "bytes": sum(item.size or 0 for item in results if item.status == "downloaded"),
            "duration": round(elapsed, 3),
            "exit_code": exit_code,
        },
        "files": [item.to_dict() for item in results],
    }


def run_check(settings: Settings, log: Logger) -> int:
    console = Console()
    console.print(f"wget2-single {__version__}")
    console.print(f"python {sys.version.split()[0]} on {sys.platform}")
    with suppress(Exception):
        console.print(f"  ✔ pycurl {pycurl.version_info()[1]}")
    console.print(f"  ✔ libcurl {pycurl.version.split()[0]}")
    directory = settings.output.directory
    try:
        ensure_directory(directory)
        probe_file = directory / ".wget2-write-test"
        probe_file.write_text("ok", encoding="utf-8")
        probe_file.unlink()
        console.print(f"  ✔ output directory writable: {directory}")
    except (OSError, Wget2Error) as exc:
        console.print(f"  ✖ output directory not writable: {directory} ({exc})")
        return EXIT_FILESYSTEM
    with suppress(OSError):
        usage = shutil.disk_usage(directory)
        console.print(f"  · free space in {directory}: {human_size(usage.free)}")
    log.debug("check complete")
    console.print("[green]OK[/green]")
    return EXIT_OK


app = typer.Typer(
    add_completion=False,
    context_settings={"help_option_names": ["-h", "--help"], "max_content_width": 130},
)


@app.command()
def main(
    urls: Optional[list[str]] = typer.Argument(None, help="URLs to download"),
    input_file: Optional[list[str]] = typer.Option(
        None, "--input-file", "-f", "--input", help="Read URLs from a file ('-' for stdin)."
    ),
    input_format: str = typer.Option(_d("input_format", "auto"), "--input-format", help="plain|json|csv|auto"),
    base_url: Optional[str] = typer.Option(
        _d("base_url", None), "--base-url", help="Resolve relative URLs against this base."
    ),
    connections: int = typer.Option(_d("connections", 8), "--connections", "-c", min=1, help="Connections per file."),
    jobs: int = typer.Option(_d("jobs", 1), "--jobs", "-j", min=1, help="Files in parallel."),
    max_active: int = typer.Option(_d("max_active", 1), "--max-active", min=1, help="Hard cap on concurrent files."),
    max_per_host: int = typer.Option(_d("max_per_host", 6), "--max-per-host", min=1, help="Connections per host."),
    retry: int = typer.Option(_d("retry", 5), "--retry", min=0, help="Retries per request."),
    retry_delay: float = typer.Option(
        _d("retry_delay", 1.0), "--retry-delay", min=0.0, help="Base retry delay in seconds."
    ),
    retry_backoff: float = typer.Option(
        _d("retry_backoff", 2.0), "--retry-backoff", min=1.0, help="Exponential backoff factor."
    ),
    retry_max_delay: float = typer.Option(
        _d("retry_max_delay", 60.0), "--retry-max-delay", min=0.0, help="Delay ceiling."
    ),
    retry_jitter: float = typer.Option(
        _d("retry_jitter", 0.25), "--retry-jitter", min=0.0, help="Random jitter fraction."
    ),
    retry_http: Optional[str] = typer.Option(
        _d("retry_http", None), "--retry-http", help="HTTP statuses to retry (comma list)."
    ),
    timeout: float = typer.Option(_d("timeout", 30.0), "--timeout", min=0.0, help="Stall timeout in seconds."),
    connect_timeout: float = typer.Option(
        _d("connect_timeout", 10.0), "--connect-timeout", min=0.0, help="Connect timeout."
    ),
    max_redirects: int = typer.Option(_d("max_redirects", 10), "--max-redirects", min=0, help="Redirect limit."),
    continue_: bool = typer.Option(
        _d("continue", False), "--continue", "-C", help="Resume from an existing .part file."
    ),
    allow_unvalidated_resume: bool = typer.Option(
        _d("allow_unvalidated_resume", False),
        "--allow-unvalidated-resume",
        help="Resume even without ETag/Last-Modified.",
    ),
    keep_partial: bool = typer.Option(
        _d("keep_partial", True), "--keep-partial/--no-keep-partial", help="Keep .part on error."
    ),
    remove_on_error: bool = typer.Option(
        _d("remove_on_error", False), "--remove-on-error", help="Delete partial data on error."
    ),
    check_disk: bool = typer.Option(
        _d("check_disk", True), "--check-disk/--no-check-disk", help="Verify free space first."
    ),
    checksum: Optional[list[str]] = typer.Option(
        _d("checksum", None), "--checksum", help="Expected digest (sha256:hex or hex)."
    ),
    checksum_file: Optional[str] = typer.Option(
        _d("checksum_file", None), "--checksum-file", help="sha256sum-style checksum file."
    ),
    insecure: bool = typer.Option(_d("insecure", False), "--insecure", "-k", help="Skip TLS verification."),
    ca_cert: Optional[str] = typer.Option(_d("ca_cert", None), "--ca-cert", help="CA bundle."),
    client_cert: Optional[str] = typer.Option(
        _d("client_cert", None), "--client-cert", help="Client certificate (PEM)."
    ),
    client_key: Optional[str] = typer.Option(_d("client_key", None), "--client-key", help="Client private key (PEM)."),
    user: Optional[str] = typer.Option(_d("user", None), "--user", help="HTTP basic user."),
    password: Optional[str] = typer.Option(_d("password", None), "--password", help="HTTP basic password."),
    bearer_token: Optional[str] = typer.Option(
        _d("bearer_token", None), "--bearer-token", help="Authorization: Bearer token."
    ),
    netrc: Optional[str] = typer.Option(_d("netrc", None), "--netrc", help="Read credentials from a netrc file."),
    header: Optional[list[str]] = typer.Option(
        _d("header", None), "--header", "-H", help="Extra request header (repeatable)."
    ),
    user_agent: Optional[str] = typer.Option(_d("user_agent", None), "--user-agent", help="UA."),
    referer: Optional[str] = typer.Option(_d("referer", None), "--referer", help="Referer."),
    cookie: Optional[list[str]] = typer.Option(
        _d("cookie", None), "--cookie", help="Cookie 'name=value' (repeatable)."
    ),
    cookie_jar: Optional[str] = typer.Option(
        _d("cookie_jar", None), "--cookie-jar", help="Load/save cookies from this file."
    ),
    method: Optional[str] = typer.Option(_d("method", None), "--method", help="HTTP method."),
    data: Optional[str] = typer.Option(
        _d("data", None), "--data", "--data-binary", help="Request body ('@file' allowed)."
    ),
    proxy: Optional[str] = typer.Option(_d("proxy", None), "--proxy", help="Proxy URL."),
    no_proxy: bool = typer.Option(_d("no_proxy", False), "--no-proxy", help="Ignore proxies."),
    http1: bool = typer.Option(_d("http1", False), "--http1", help="Disable HTTP/2."),
    output: Optional[str] = typer.Option(_d("output", None), "--output", "-O", help="Exact output path (single URL)."),
    output_template: Optional[str] = typer.Option(
        _d("output_template", None),
        "--output-template",
        help="Per-file path template, e.g. {host}/{filename}.",
    ),
    output_dir: str = typer.Option(_d("output_dir", "."), "--output-dir", "-P", help="Destination directory."),
    no_clobber: bool = typer.Option(_d("no_clobber", False), "--no-clobber", "-nc", help="Skip existing files."),
    overwrite: bool = typer.Option(_d("overwrite", True), "--overwrite/--no-overwrite", help="Replace existing files."),
    timestamping: bool = typer.Option(_d("timestamping", False), "--timestamping", "-N", help="Skip up-to-date files."),
    unique: bool = typer.Option(_d("unique", False), "--unique", help="Never overwrite; add a numeric suffix."),
    content_disposition: bool = typer.Option(
        _d("content_disposition", True),
        "--content-disposition/--no-content-disposition",
        help="Honour Content-Disposition filenames.",
    ),
    manifest: bool = typer.Option(_d("manifest", False), "--manifest", help="Write a .wget2.json sidecar per file."),
    max_filesize: Optional[str] = typer.Option(
        _d("max_filesize", None), "--max-filesize", help="Reject resources larger than this."
    ),
    rate_limit: Optional[str] = typer.Option(
        _d("rate_limit", None), "--rate-limit", help="Global rate limit, e.g. 5M."
    ),
    rate_limit_per_connection: Optional[str] = typer.Option(
        _d("rate_limit_per_connection", None),
        "--rate-limit-per-connection",
        help="Per-connection rate limit.",
    ),
    buffer_threshold: Optional[str] = typer.Option(
        _d("buffer_threshold", None), "--buffer-threshold", help="Buffer files up to this size."
    ),
    mirror_list: Optional[list[str]] = typer.Option(
        _d("mirror_list", None),
        "--mirror-list",
        help="Fallback URL(s) tried in order on failure.",
    ),
    recursive: bool = typer.Option(_d("recursive", False), "--recursive", "-r", help="Follow links on HTML pages."),
    mirror: bool = typer.Option(_d("mirror", False), "--mirror", "-m", help="Recursive mirroring (implies -r -N)."),
    level: int = typer.Option(_d("level", 5), "--level", "-l", min=0, help="Crawl depth."),
    spider: bool = typer.Option(_d("spider", False), "--spider", help="Only check URLs, write nothing."),
    accept: Optional[list[str]] = typer.Option(_d("accept", None), "--accept", "-A", help="Filename globs to accept."),
    reject: Optional[list[str]] = typer.Option(_d("reject", None), "--reject", "-R", help="Filename globs to reject."),
    include: Optional[list[str]] = typer.Option(_d("include", None), "--include", "-I", help="URL globs to include."),
    exclude: Optional[list[str]] = typer.Option(_d("exclude", None), "--exclude", "-X", help="URL globs to exclude."),
    dry_run: bool = typer.Option(_d("dry_run", False), "--dry-run", help="Probe only (HEAD), download nothing."),
    json_report: bool = typer.Option(_d("json", False), "--json", help="Emit a JSON report on stdout."),
    quiet: bool = typer.Option(_d("quiet", False), "--quiet", "-q", help="Only errors."),
    verbose: bool = typer.Option(_d("verbose", False), "--verbose", "-v", help="Debug logging."),
    progress: str = typer.Option(_d("progress", "rich"), "--progress", help="rich|plain|none"),
    no_progress: bool = typer.Option(_d("no_progress", False), "--no-progress", help="Disable the progress display."),
    log_file: Optional[str] = typer.Option(_d("log_file", None), "--log-file", help="Log file."),
    log_level: str = typer.Option(_d("log_level", "info"), "--log-level", help="debug|info|warning|error"),
    json_logs: bool = typer.Option(_d("json_logs", False), "--json-logs", help="Emit log lines as JSON."),
    allow_unsafe_schemes: bool = typer.Option(
        _d("allow_unsafe_schemes", False),
        "--allow-unsafe-schemes",
        help="Permit file://, ftp:// and ftps://.",
    ),
    config: Optional[str] = typer.Option(None, "--config", help="TOML config file (defaults are auto-discovered)."),
    no_config: bool = typer.Option(False, "--no-config", help="Ignore config files and WGET2_*."),
    check: bool = typer.Option(False, "--check", help="Verify the environment and exit."),
    version: bool = typer.Option(False, "--version", help="Print the version and exit."),
) -> None:
    if version:
        typer.echo(f"wget2-single {__version__}")
        raise typer.Exit(EXIT_OK)
    del config, no_config
    effective_level = "debug" if verbose else log_level
    log = Logger(
        effective_level,
        path=Path(log_file) if log_file else None,
        quiet=quiet,
        json_logs=json_logs,
    )
    interrupt = InterruptFlag()
    cookies: list[str] = [item for item in (cookie or ()) if item]
    options: dict[str, Any] = {
        "connections": connections,
        "jobs": jobs,
        "max_active": max_active,
        "max_per_host": max_per_host,
        "retry": retry,
        "retry_delay": retry_delay,
        "retry_backoff": retry_backoff,
        "retry_max_delay": retry_max_delay,
        "retry_jitter": retry_jitter,
        "retry_http": retry_http,
        "timeout": timeout,
        "connect_timeout": connect_timeout,
        "max_redirects": max_redirects,
        "continue_": continue_,
        "allow_unvalidated_resume": allow_unvalidated_resume,
        "keep_partial": keep_partial,
        "remove_on_error": remove_on_error,
        "check_disk": check_disk,
        "checksum": checksum,
        "checksum_file": checksum_file,
        "insecure": insecure,
        "ca_cert": ca_cert,
        "client_cert": client_cert,
        "client_key": client_key,
        "user": user,
        "password": password,
        "bearer_token": bearer_token,
        "netrc": netrc,
        "header": header,
        "user_agent": user_agent,
        "referer": referer,
        "cookie": cookies,
        "cookie_jar": cookie_jar,
        "method": method,
        "data": data,
        "proxy": proxy,
        "no_proxy": no_proxy,
        "http1": http1,
        "output": output,
        "output_template": output_template,
        "output_dir": output_dir,
        "no_clobber": no_clobber,
        "overwrite": overwrite,
        "timestamping": timestamping,
        "unique": unique,
        "content_disposition": content_disposition,
        "manifest": manifest,
        "max_filesize": max_filesize,
        "rate_limit": rate_limit,
        "rate_limit_per_connection": rate_limit_per_connection,
        "buffer_threshold": buffer_threshold,
        "mirror_list": mirror_list,
        "recursive": recursive or mirror,
        "mirror": mirror,
        "spider": spider,
        "accept": accept,
        "reject": reject,
        "include": include,
        "exclude": exclude,
        "dry_run": dry_run,
        "allow_unsafe_schemes": allow_unsafe_schemes,
    }
    try:
        settings = _build_settings(options)
        if cookie:
            settings = replace(
                settings,
                request=replace(
                    settings.request,
                    headers=settings.request.headers + (("Cookie", "; ".join(cookies)),),
                ),
            )
        if mirror:
            settings = replace(
                settings,
                output=replace(settings.output, timestamping=True),
            )
        if checksum_file:
            log.debug("loaded checksum file", entries=len(settings.checksum_table))
    except (UsageError, ConfigError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise typer.Exit(EXIT_USAGE) from exc
    if check:
        raise typer.Exit(run_check(settings, log))
    mode = "none" if (no_progress or json_report or quiet) else progress
    if mode not in {"rich", "plain", "none"}:
        print(f"error: invalid --progress value {mode!r}", file=sys.stderr)
        raise typer.Exit(EXIT_USAGE)
    target_urls = list(urls or ())
    if input_file:
        try:
            target_urls.extend(_read_input(input_file, input_format, base_url))
        except (ConfigError, UsageError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            raise typer.Exit(EXIT_USAGE) from exc
    if base_url:
        target_urls = [item if urlparse(item).scheme else urljoin(base_url, item) for item in target_urls]
    if not target_urls:
        print("error: no URLs given", file=sys.stderr)
        raise typer.Exit(EXIT_USAGE)
    try:
        for item in target_urls:
            validate_url(item, allow_extra_schemes=allow_unsafe_schemes)
    except Wget2Error as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise typer.Exit(exc.exit_code) from exc
    if output and len(target_urls) > 1 and not output_template:
        print(
            "error: --output/-O can only be used with a single URL; use --output-template",
            file=sys.stderr,
        )
        raise typer.Exit(EXIT_USAGE)
    jar = CookieJar()
    if cookie_jar and Path(cookie_jar).exists():
        try:
            jar.load_file(Path(cookie_jar))
        except ConfigError as exc:
            print(f"error: {exc}", file=sys.stderr)
            raise typer.Exit(EXIT_USAGE) from exc
    progress_reporter = ProgressReporter(mode, quiet=quiet, overall=len(target_urls) > 1 and mode == "rich")
    log.line_hook = progress_reporter.clear_line
    restore = _install_interrupt(interrupt)
    started = time.monotonic()
    results: list[Result] = []
    exit_code = EXIT_OK
    try:
        runner = Runner(settings, log, progress_reporter, interrupt)
        runner.cookies = jar
        crawl_seeds = target_urls
        if settings.recursive or settings.spider:
            crawler = Crawler(settings, log, runner._client, level)
            discovered = crawler.discover(crawl_seeds)
            if discovered:
                target_urls = discovered
            log.info("crawl finished", urls=len(target_urls))
        results = runner.run(target_urls)
        exit_code = _summary_exit(results, interrupt.is_set())
    except (Interrupted, KeyboardInterrupt):
        exit_code = EXIT_INTERRUPTED
        log.error("interrupted")
    except Wget2Error as exc:
        log.error("failed", error=str(exc))
        exit_code = exc.exit_code
    except OSError as exc:
        log.error("filesystem failure", error=str(exc))
        exit_code = EXIT_FILESYSTEM
    finally:
        progress_reporter.close()
        restore()
        log.close()
    elapsed = time.monotonic() - started
    if cookie_jar:
        with suppress(Wget2Error):
            jar.save(Path(cookie_jar))
    if json_report:
        typer.echo(json.dumps(_report(results, elapsed, exit_code), indent=2))
    elif not quiet:
        console = Console(stderr=True)
        _print_summary(results, elapsed, console)
    log.info(
        "batch finished",
        downloaded=sum(1 for item in results if item.status == "downloaded"),
        skipped=sum(1 for item in results if item.status in {"skipped", "up-to-date"}),
        failed=sum(1 for item in results if item.status == "failed"),
        exit_code=exit_code,
    )
    raise typer.Exit(exit_code)


if __name__ == "__main__":
    app()
