from __future__ import annotations

import pytest

from wget2.errors import HTTPStatusError, NetworkError, TransportError
from wget2.retry import RetryPolicy, call_with_retries, parse_retry_status


class TestDelays:
    def test_exponential(self) -> None:
        policy = RetryPolicy(max_retries=5, delay=1.0, backoff=2.0, jitter=0.0, max_delay=100.0)
        assert [policy.delay_for(i) for i in (1, 2, 3)] == [1.0, 2.0, 4.0]

    def test_capped(self) -> None:
        policy = RetryPolicy(max_retries=10, delay=1.0, backoff=10.0, jitter=0.0, max_delay=5.0)
        assert policy.delay_for(5) == 5.0

    def test_jitter_bounds(self) -> None:
        policy = RetryPolicy(delay=2.0, jitter=0.5, backoff=1.0)
        for _ in range(50):
            assert 1.0 <= policy.delay_for(1) <= 3.0

    def test_zero_delay(self) -> None:
        assert RetryPolicy(delay=0.0, jitter=0.0).delay_for(3) == 0.0


class TestRetryability:
    def test_http_status(self) -> None:
        policy = RetryPolicy(retry_status=frozenset({503}))
        assert policy.should_retry(HTTPStatusError(503, "boom"))
        assert not policy.should_retry(HTTPStatusError(404, "missing"))

    def test_network(self) -> None:
        policy = RetryPolicy()
        assert policy.should_retry(TransportError("reset"))
        assert not policy.should_retry(ValueError("nope"))

    def test_non_retryable_network(self) -> None:
        policy = RetryPolicy()
        assert not policy.should_retry(NetworkError("x", retryable=False))

    def test_parse_status_list(self) -> None:
        assert parse_retry_status("429,500-502") == frozenset({429, 500, 501, 502})
        assert parse_retry_status("") == frozenset()
        assert 503 in parse_retry_status(None)


class TestCallWithRetries:
    def test_succeeds_after_failures(self) -> None:
        attempts = {"n": 0}
        sleeps: list[float] = []

        def flaky() -> str:
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise TransportError("temporary")
            return "ok"

        policy = RetryPolicy(max_retries=5, delay=0.01, jitter=0.0)
        assert call_with_retries(flaky, policy=policy, sleep=sleeps.append) == "ok"
        assert attempts["n"] == 3
        assert len(sleeps) == 2

    def test_exhausted(self) -> None:
        def always() -> None:
            raise TransportError("nope")

        policy = RetryPolicy(max_retries=2, delay=0.0, jitter=0.0)
        with pytest.raises(TransportError):
            call_with_retries(always, policy=policy, sleep=lambda _: None)

    def test_non_retryable_is_not_retried(self) -> None:
        calls = {"n": 0}

        def boom() -> None:
            calls["n"] += 1
            raise HTTPStatusError(404, "missing")

        policy = RetryPolicy(max_retries=5)
        with pytest.raises(HTTPStatusError):
            call_with_retries(boom, policy=policy, sleep=lambda _: None)
        assert calls["n"] == 1

    def test_on_retry_callback(self) -> None:
        seen: list[int] = []

        def flaky() -> str:
            if len(seen) < 1:
                raise TransportError("x")
            return "done"

        call_with_retries(
            flaky,
            policy=RetryPolicy(max_retries=2, delay=0.0, jitter=0.0),
            on_retry=lambda attempt, delay, exc: seen.append(attempt),
            sleep=lambda _: None,
        )
        assert seen == [1]
