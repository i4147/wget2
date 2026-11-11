"""Token-bucket rate limiting (global aggregate and per-connection)."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterable


class RateLimiter:
    """A thread-safe token bucket.

    ``rate`` is in bytes/second; ``None`` disables limiting.
    """

    def __init__(
        self,
        rate: float | None,
        *,
        burst: float | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.rate = float(rate) if rate else None
        self.capacity = float(burst) if burst else (self.rate or 0.0)
        self._tokens = self.capacity
        self._updated = clock()
        self._lock = threading.Lock()
        self._clock = clock
        self._sleep = time.sleep

    @property
    def enabled(self) -> bool:
        return bool(self.rate and self.rate > 0)

    def _refill(self) -> None:
        now = self._clock()
        elapsed = max(0.0, now - self._updated)
        self._updated = now
        rate = self.rate
        if rate and self.capacity > 0:
            self._tokens = min(self.capacity, self._tokens + elapsed * rate)

    def consume(self, amount: int) -> None:
        """Account for ``amount`` bytes, sleeping if we are over budget."""
        rate = self.rate
        if not rate or amount <= 0:
            return
        with self._lock:
            self._refill()
            self._tokens -= amount
            deficit = -self._tokens
        if deficit > 0:
            time.sleep(deficit / rate)
            with self._lock:
                self._refill()

    # convenience alias
    def throttle(self, amount: int) -> None:
        self.consume(amount)


class MultiLimiter:
    """Combine several limiters (e.g. global + per-connection)."""

    def __init__(self, limiters: Iterable[RateLimiter | None]) -> None:
        self.limiters = [item for item in limiters if item is not None and item.enabled]

    @property
    def enabled(self) -> bool:
        return bool(self.limiters)

    def consume(self, amount: int) -> None:
        for limiter in self.limiters:
            limiter.consume(amount)

    throttle = consume


def parse_rate(value: str | int | float | None) -> float | None:
    """Parse a ``--rate-limit`` value into bytes/second."""
    from .util import parse_size

    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) or None
    text = str(value).strip().lower()
    if not text or text in {"0", "none", "off", "unlimited"}:
        return None
    if text.endswith("/s"):
        text = text[:-2]
    size = parse_size(text)
    return float(size) if size else None
