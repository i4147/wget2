"""Retry policy with exponential backoff, jitter and HTTP-status filtering."""

from __future__ import annotations

import random
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import TypeVar

from .errors import HTTPStatusError, NetworkError, Wget2Error
from .log import get_logger

T = TypeVar("T")

DEFAULT_RETRY_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})


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
        """Delay before retry number ``attempt`` (1-based)."""
        exp = min(self.max_delay, max(0.0, self.delay) * (self.backoff ** max(0, attempt - 1)))
        if self.jitter > 0:
            spread = exp * self.jitter
            exp = random.uniform(max(0.0, exp - spread), exp + spread)
        return max(0.0, exp)

    def should_retry(self, exc: BaseException) -> bool:
        if isinstance(exc, HTTPStatusError):
            return (exc.status_code or 0) in self.retry_status or exc.retryable
        if isinstance(exc, NetworkError):
            return exc.retryable
        return False


def parse_retry_status(value: str | Iterable[int] | None) -> frozenset[int]:
    from .util import parse_int_list

    if value is None:
        return DEFAULT_RETRY_STATUS
    if isinstance(value, str):
        parsed = parse_int_list(value)
        # an explicitly empty string means "never retry on status"
        return parsed if parsed else frozenset()
    return frozenset(int(v) for v in value)


def call_with_retries(
    func: Callable[[], T],
    *,
    policy: RetryPolicy,
    on_retry: Callable[[int, float, BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    should_abort: Callable[[], bool] | None = None,
    description: str = "",
) -> T:
    """Call ``func`` until it succeeds or the policy is exhausted.

    Only errors flagged retryable (network errors / configured HTTP statuses)
    trigger another attempt; everything else propagates immediately.
    """
    logger = get_logger("retry")
    last_exc: BaseException | None = None
    for attempt in range(1, policy.attempts + 1):
        if should_abort is not None and should_abort():
            raise Wget2Error("aborted")
        try:
            return func()
        except Wget2Error as exc:
            last_exc = exc
            if not policy.should_retry(exc) or attempt >= policy.attempts:
                raise
            delay = policy.delay_for(attempt)
            logger.debug(
                "retrying after error",
                attempt=attempt,
                of=policy.attempts - 1,
                delay=f"{delay:.2f}s",
                error=str(exc),
                target=description,
            )
            if on_retry is not None:
                on_retry(attempt, delay, exc)
            sleep(delay)
    raise last_exc if last_exc else Wget2Error("retry loop exhausted")  # pragma: no cover
