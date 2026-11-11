from __future__ import annotations

import time

import pytest

from wget2.ratelimit import MultiLimiter, RateLimiter, parse_rate


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture()
def sleeper(monkeypatch):
    recorded: list[float] = []
    clock = FakeClock()

    def fake_sleep(seconds: float) -> None:
        recorded.append(seconds)
        clock.advance(seconds)

    monkeypatch.setattr(time, "sleep", fake_sleep)
    return recorded, clock


class TestRateLimiter:
    def test_disabled(self, sleeper) -> None:
        recorded, _ = sleeper
        limiter = RateLimiter(None)
        assert not limiter.enabled
        limiter.consume(10**9)
        assert recorded == []

    def test_burst_then_throttle(self, sleeper) -> None:
        recorded, _ = sleeper
        limiter = RateLimiter(1000, clock=sleeper[1])
        limiter.consume(600)
        assert recorded == []
        limiter.consume(600)  # 1200 > 1000 tokens
        assert recorded and recorded[-1] > 0

    def test_refill(self, sleeper) -> None:
        recorded, _ = sleeper
        limiter = RateLimiter(1000, clock=sleeper[1])
        limiter.consume(1000)
        sleeper[1].advance(1.0)  # a full second of tokens
        limiter.consume(500)
        assert recorded == []

    def test_approximate_total_time(self, sleeper) -> None:
        recorded, _ = sleeper
        limiter = RateLimiter(100, burst=100, clock=sleeper[1])
        for _ in range(10):
            limiter.consume(100)
        # 900 bytes over the burst => about 9 seconds of sleeping
        assert sum(recorded) == pytest.approx(9.0, abs=0.5)

    def test_multi_limiter(self, sleeper) -> None:
        recorded, clock = sleeper
        a = RateLimiter(1000, clock=clock)
        b = RateLimiter(1000, clock=clock)
        multi = MultiLimiter([a, b])
        assert multi.enabled
        multi.consume(900)
        multi.consume(900)
        assert sum(recorded) > 0

    def test_parse_rate(self) -> None:
        assert parse_rate("5M") == 5 * 1024 * 1024
        assert parse_rate("1024") == 1024.0
        assert parse_rate("2K/s") == 2048.0
        assert parse_rate(None) is None
        assert parse_rate("0") is None
        assert parse_rate(1000) == 1000.0
