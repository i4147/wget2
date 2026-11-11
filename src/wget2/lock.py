"""Single-instance guard via an advisory lock file."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import IO, Any

from .errors import FilesystemError, LockError
from .log import get_logger
from .util import safe_unlink

#: the two platform locking modules, whichever is importable (typed loosely so
#: that one code path can reference both)
_fcntl: Any = None
_msvcrt: Any = None

if sys.platform == "win32":  # pragma: no cover - windows only
    import msvcrt

    _msvcrt = msvcrt
else:
    try:
        import fcntl

        _fcntl = fcntl
    except ImportError:  # pragma: no cover - exotic platforms
        _fcntl = None


class FileLock:
    """Advisory exclusive lock, released automatically when the process dies."""

    def __init__(self, path: Path, *, enabled: bool = True) -> None:
        self.path = path
        self.enabled = enabled
        self._handle: IO[str] | None = None
        self.log = get_logger("lock")

    def acquire(self) -> None:
        if not self.enabled:
            return
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            handle = self.path.open("a+")
        except OSError as exc:
            raise FilesystemError(f"cannot create lock file {self.path}: {exc}") from exc
        try:
            if _fcntl is not None:
                _fcntl.flock(handle.fileno(), _fcntl.LOCK_EX | _fcntl.LOCK_NB)
            elif _msvcrt is not None:  # pragma: no cover - windows
                _msvcrt.locking(handle.fileno(), _msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            owner = ""
            try:
                handle.seek(0)
                owner = handle.read().strip()
            except OSError:  # pragma: no cover
                pass
            handle.close()
            raise LockError(
                f"another instance is running (lock file {self.path}"
                + (f", pid {owner}" if owner else "")
                + "); use --no-lock to override"
            ) from exc
        try:
            handle.seek(0)
            handle.truncate()
            handle.write(f"{os.getpid()}\n")
            handle.flush()
        except OSError:  # pragma: no cover - best effort
            pass
        self._handle = handle
        self.log.debug("lock acquired", path=str(self.path), pid=os.getpid())

    def release(self) -> None:
        handle = self._handle
        self._handle = None
        if handle is None:
            return
        try:
            if _fcntl is not None:
                _fcntl.flock(handle.fileno(), _fcntl.LOCK_UN)
            elif _msvcrt is not None:  # pragma: no cover - windows
                try:
                    handle.seek(0)
                    _msvcrt.locking(handle.fileno(), _msvcrt.LK_UNLCK, 1)
                except OSError:
                    pass
        finally:
            try:
                handle.close()
            except OSError:  # pragma: no cover
                pass
        safe_unlink(self.path)

    def __enter__(self) -> FileLock:
        self.acquire()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.release()
