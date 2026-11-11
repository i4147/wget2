"""Progress reporting: rich, plain or none — with batch aggregation."""

from __future__ import annotations

import sys
import threading
import time
from typing import Any, TextIO

from .util import human_size

MODES = ("rich", "plain", "none")


class TaskHandle:
    """Opaque handle returned by :meth:`ProgressReporter.add_file`."""

    __slots__ = ("id", "description", "total")

    def __init__(self, identifier: int, description: str, total: int | None) -> None:
        self.id = identifier
        self.description = description
        self.total = total


class ProgressReporter:
    """Base reporter: per-file tasks plus one aggregate task."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._next_id = 0
        self._started = 0.0

    # -- lifecycle -------------------------------------------------------- #
    def start(self, *, total_files: int, total_bytes: int | None = None) -> None:
        self._started = time.monotonic()

    def add_file(self, description: str, total: int | None, *, already_done: int = 0) -> TaskHandle:
        with self._lock:
            handle = TaskHandle(self._next_id, description, total)
            self._next_id += 1
        return handle

    def update(self, handle: TaskHandle, advance: int = 0, *, description: str | None = None) -> None:
        pass

    def finish(self, handle: TaskHandle, *, status: str = "done") -> None:
        pass

    def log(self, message: str) -> None:
        pass

    def close(self) -> None:
        pass

    def __enter__(self) -> ProgressReporter:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


class NullReporter(ProgressReporter):
    pass


class PlainReporter(ProgressReporter):
    """Line-oriented output for CI logs: one line per file, throttled."""

    def __init__(self, stream: TextIO | None = None, *, interval: float = 1.0) -> None:
        super().__init__()
        self.stream = stream or sys.stderr
        self.interval = interval
        self._state: dict[int, dict[str, float]] = {}
        self._last_line = 0.0

    def add_file(self, description: str, total: int | None, *, already_done: int = 0) -> TaskHandle:
        handle = super().add_file(description, total)
        with self._lock:
            self._state[handle.id] = {
                "done": float(already_done),
                "total": float(total or 0),
                "last": 0.0,
                "began": time.monotonic(),
            }
            self._emit(handle, already_done=already_done, force=True)
        return handle

    def update(self, handle: TaskHandle | None, advance: int = 0, *, description: str | None = None) -> None:
        if handle is None:
            return
        with self._lock:
            state = self._state.get(handle.id)
            if state is None:
                return
            state["done"] += advance
            now = time.monotonic()
            if now - state["last"] >= self.interval:
                state["last"] = now
                self._emit(handle, already_done=int(state["done"]))

    def _emit(self, handle: TaskHandle, *, already_done: int, force: bool = False) -> None:
        state = self._state.get(handle.id, {})
        total = state.get("total") or None
        done = state.get("done", float(already_done))
        elapsed = max(1e-6, time.monotonic() - state.get("began", time.monotonic()))
        speed = done / elapsed
        if total:
            percent = 100.0 * done / total
            line = (
                f"{handle.description}: {percent:5.1f}% "
                f"{human_size(done)}/{human_size(total)} at {human_size(speed)}/s"
            )
        else:
            line = f"{handle.description}: {human_size(done)} at {human_size(speed)}/s"
        print(line, file=self.stream, flush=True)

    def finish(self, handle: TaskHandle | None, *, status: str = "done") -> None:
        if handle is None:
            return
        with self._lock:
            state = self._state.get(handle.id)
            if state is None:
                return
            done = int(state.get("done", 0))
            elapsed = max(1e-6, time.monotonic() - state.get("began", time.monotonic()))
            print(
                f"{handle.description}: {status} {human_size(done)} in {elapsed:.1f}s",
                file=self.stream,
                flush=True,
            )
            self._state.pop(handle.id, None)

    def log(self, message: str) -> None:
        print(message, file=self.stream, flush=True)


class RichReporter(ProgressReporter):
    """Rich-based bars with an aggregate task for multi-URL batches."""

    def __init__(self, *, show_overall: bool = False) -> None:
        super().__init__()
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

        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}", table_column=None),
            BarColumn(bar_width=None),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=Console(stderr=True),
            transient=False,
            auto_refresh=True,
            refresh_per_second=10,
        )
        self._progress.start()
        self._handles: dict[int, Any] = {}
        self._overall: Any = None

    def start(self, *, total_files: int, total_bytes: int | None = None) -> None:
        super().start(total_files=total_files, total_bytes=total_bytes)
        if total_files > 1:
            self._overall = self._progress.add_task(
                f"[bold]total ({total_files} files)", total=total_bytes or None
            )

    def add_file(self, description: str, total: int | None, *, already_done: int = 0) -> TaskHandle:
        handle = super().add_file(description, total)
        with self._lock:
            task_id = self._progress.add_task(description, total=total, completed=already_done)
            self._handles[handle.id] = task_id
            if self._overall is not None:
                overall = self._progress.tasks[self._overall]
                if already_done:
                    self._progress.update(self._overall, advance=already_done)
                elif total and overall.total:
                    self._progress.update(self._overall, total=overall.total + total)
                elif total:
                    self._progress.update(self._overall, total=total)
        return handle

    def update(self, handle: TaskHandle | None, advance: int = 0, *, description: str | None = None) -> None:
        if handle is None:
            return
        task_id = self._handles.get(handle.id)
        if task_id is None:
            return
        with self._lock:
            if description:
                self._progress.update(task_id, description=description)
            if advance:
                self._progress.update(task_id, advance=advance)
                if self._overall is not None:
                    self._progress.update(self._overall, advance=advance)

    def finish(self, handle: TaskHandle | None, *, status: str = "done") -> None:
        if handle is None:
            return
        task_id = self._handles.get(handle.id)
        if task_id is None:
            return
        with self._lock:
            self._progress.update(task_id, description=f"{handle.description} [{status}]")
            task = self._progress.tasks[task_id]
            if task.total and task.completed < task.total and status == "done":
                self._progress.update(task_id, completed=task.total)

    def log(self, message: str) -> None:
        with self._lock:
            self._progress.console.print(message, highlight=False)

    def close(self) -> None:
        with self._lock:
            try:
                self._progress.stop()
            except Exception:  # pragma: no cover - defensive
                pass


def make_reporter(mode: str, *, show_overall: bool = False) -> ProgressReporter:
    normalized = (mode or "rich").lower()
    if normalized == "none":
        return NullReporter()
    if normalized == "plain":
        return PlainReporter()
    if normalized == "rich":
        try:
            import rich  # noqa: F401
        except ImportError:  # pragma: no cover - rich is a hard dependency
            return PlainReporter()
        return RichReporter(show_overall=show_overall)
    raise ValueError(f"unknown progress mode: {mode}")
