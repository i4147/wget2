"""Range planning, byte-coverage tracking and resumable ``.part`` state."""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from .errors import FilesystemError, UsageError
from .log import get_logger
from .util import fsync_dir, safe_unlink

STATE_VERSION = 1
#: minimum bytes per range when splitting a download
MIN_RANGE_SIZE = 64 * 1024

Interval = tuple[int, int]
"""A half-open byte interval ``[start, end)``."""

STATE_SUFFIX = ".json"


def plan_ranges(size: int, connections: int, *, min_range: int = MIN_RANGE_SIZE) -> list[Interval]:
    """Split ``[0, size)`` into ``connections`` contiguous, balanced ranges."""
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
    ordered = sorted((int(s), int(e)) for s, e in intervals if e > s)
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
    """Return the parts of ``interval`` not covered by ``covered``."""
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


@dataclass
class PartState:
    """State persisted next to a ``.part`` file so downloads can resume."""

    part_path: Path
    url: str
    size: int | None
    etag: str | None = None
    last_modified: str | None = None
    plan: list[Interval] = field(default_factory=list)
    covered: list[Interval] = field(default_factory=list)
    backend: str | None = None
    created: float = field(default_factory=time.time)
    updated: float = field(default_factory=time.time)
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False, compare=False)
    _dirty: bool = field(default=False, repr=False, compare=False)
    _last_save: float = field(default=0.0, repr=False, compare=False)

    # -- paths ------------------------------------------------------------ #
    @property
    def state_path(self) -> Path:
        return self.part_path.with_name(self.part_path.name + STATE_SUFFIX)

    # -- persistence ------------------------------------------------------ #
    @classmethod
    def state_file_for(cls, part_path: Path) -> Path:
        return part_path.with_name(part_path.name + STATE_SUFFIX)

    @classmethod
    def load(cls, part_path: Path) -> PartState | None:
        state_path = cls.state_file_for(part_path)
        if not state_path.exists():
            return None
        try:
            payload = json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            get_logger("state").warning("discarding unreadable part state", path=str(state_path), error=str(exc))
            safe_unlink(state_path)
            return None
        if payload.get("version") != STATE_VERSION:
            get_logger("state").info("ignoring part state with old version", path=str(state_path))
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
                backend=payload.get("backend"),
                created=payload.get("created", time.time()),
                updated=payload.get("updated", time.time()),
            )
        except (TypeError, ValueError) as exc:  # pragma: no cover - defensive
            get_logger("state").warning("invalid part state", path=str(state_path), error=str(exc))
            safe_unlink(state_path)
            return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": STATE_VERSION,
            "url": self.url,
            "size": self.size,
            "etag": self.etag,
            "last_modified": self.last_modified,
            "plan": [list(item) for item in self.plan],
            "covered": [list(item) for item in self.covered],
            "backend": self.backend,
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
        # unique temp name: several range workers may checkpoint concurrently
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

    # -- coverage --------------------------------------------------------- #
    def mark(self, start: int, end: int, *, interval: float = 2.0) -> None:
        if end <= start:
            return
        with self._lock:
            self.covered = merge_intervals([*self.covered, (start, end)])
            self._dirty = True
        self.maybe_save(interval)

    def resume_offset(self, start: int, end: int) -> int:
        """Contiguous prefix of ``[start, end)`` that is already on disk."""
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

    def complete(self) -> bool:
        if self.size is None:
            return False
        with self._lock:
            return covered_bytes(self.covered) >= self.size and not self.missing()

    # -- validation ------------------------------------------------------- #
    def matches_remote(
        self,
        *,
        size: int | None,
        etag: str | None,
        last_modified: str | None,
        allow_unvalidated: bool = False,
    ) -> tuple[bool, str]:
        """Decide whether an existing ``.part`` may be resumed."""
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
    backend: str | None,
    resume: bool,
    allow_unvalidated: bool = False,
) -> tuple[PartState, bool]:
    """Load or create the state object. Returns ``(state, resumed)``."""
    existing = PartState.load(part_path) if resume else None
    if existing is not None and not part_path.exists():
        get_logger("state").info("ignoring state without a part file", path=str(part_path))
        existing.discard()
        existing = None
    if existing is not None and existing.url and url and existing.url != url:
        get_logger("state").warning(
            "restarting download",
            path=str(part_path),
            reason="partial file belongs to a different URL",
        )
        existing.discard()
        existing = None
    if existing is not None:
        ok, reason = existing.matches_remote(
            size=size, etag=etag, last_modified=last_modified, allow_unvalidated=allow_unvalidated
        )
        if ok:
            get_logger("state").info(
                "resuming partial download",
                path=str(part_path),
                resume=reason,
                done_bytes=existing.done_bytes(),
                size=size,
            )
            existing.url = url
            existing.backend = backend
            existing.maybe_save(0.0)
            return existing, True
        get_logger("state").warning("restarting download", path=str(part_path), reason=reason)
        existing.discard()

    state = PartState(
        part_path=part_path,
        url=url,
        size=size,
        etag=etag,
        last_modified=last_modified,
        plan=[(int(item[0]), int(item[1])) for item in plan],
        covered=[],
        backend=backend,
    )
    return state, False


def ensure_directory(path: Path) -> None:
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise FilesystemError(f"cannot create directory {path}: {exc}") from exc


def validate_template_plan(size: int, connections: int) -> list[Interval]:  # pragma: no cover - compat
    if size < 0:
        raise UsageError("negative size")
    return plan_ranges(size, connections)
