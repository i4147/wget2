from __future__ import annotations

from pathlib import Path

from wget2.state import (
    PartState,
    covered_bytes,
    merge_intervals,
    open_part_state,
    plan_ranges,
    subtract_interval,
)


class TestPlanRanges:
    def test_balanced(self) -> None:
        ranges = plan_ranges(1000, 4, min_range=1)
        assert len(ranges) == 4
        assert ranges[0][0] == 0
        assert ranges[-1][1] == 1000
        assert covered_bytes(ranges) == 1000
        for (_, end), (start, _) in zip(ranges, ranges[1:], strict=False):
            assert end == start

    def test_small_file_single_range(self) -> None:
        assert plan_ranges(10, 8, min_range=1024) == [(0, 10)]

    def test_zero_size(self) -> None:
        assert plan_ranges(0, 8) == []

    def test_never_exceeds_connections(self) -> None:
        ranges = plan_ranges(10_000, 3, min_range=1)
        assert len(ranges) == 3


class TestIntervals:
    def test_merge(self) -> None:
        assert merge_intervals([(0, 5), (3, 8), (10, 12)]) == [(0, 8), (10, 12)]

    def test_covered(self) -> None:
        assert covered_bytes([(0, 5), (3, 8)]) == 8

    def test_subtract(self) -> None:
        assert subtract_interval((0, 10), [(0, 3), (7, 10)]) == [(3, 7)]
        assert subtract_interval((0, 10), [(0, 10)]) == []
        assert subtract_interval((0, 10), []) == [(0, 10)]
        assert subtract_interval((0, 10), [(4, 6)]) == [(0, 4), (6, 10)]


class TestPartState:
    def _make(self, tmp_path: Path, size: int = 100) -> PartState:
        return PartState(
            part_path=tmp_path / "file.part",
            url="https://example.com/file",
            size=size,
            etag='"abc"',
            last_modified="Wed, 21 Oct 2015 07:28:00 GMT",
            plan=[(0, 50), (50, 100)],
        )

    def test_save_and_load(self, tmp_path: Path) -> None:
        state = self._make(tmp_path)
        state.mark(0, 25)
        state.save()
        loaded = PartState.load(state.part_path)
        assert loaded is not None
        assert loaded.size == 100
        assert loaded.etag == '"abc"'
        assert loaded.done_bytes() == 25
        assert loaded.plan == [(0, 50), (50, 100)]

    def test_missing(self, tmp_path: Path) -> None:
        state = self._make(tmp_path)
        state.mark(0, 25)
        state.mark(50, 100)
        assert state.missing() == [(25, 50)]

    def test_resume_offset(self, tmp_path: Path) -> None:
        state = self._make(tmp_path)
        state.mark(0, 30)
        assert state.resume_offset(0, 50) == 30
        assert state.resume_offset(50, 100) == 50
        state.mark(50, 60)
        assert state.resume_offset(50, 100) == 60

    def test_complete(self, tmp_path: Path) -> None:
        state = self._make(tmp_path)
        assert not state.complete()
        state.mark(0, 100)
        assert state.complete()

    def test_validators(self, tmp_path: Path) -> None:
        state = self._make(tmp_path)
        ok, reason = state.matches_remote(
            size=100, etag='"abc"', last_modified="Wed, 21 Oct 2015 07:28:00 GMT"
        )
        assert ok, reason

        ok, reason = state.matches_remote(size=200, etag='"abc"', last_modified=None)
        assert not ok and "size" in reason

        ok, reason = state.matches_remote(size=100, etag='"other"', last_modified=None)
        assert not ok and "ETag" in reason

        ok, reason = state.matches_remote(
            size=100, etag='"abc"', last_modified="Thu, 22 Oct 2015 07:28:00 GMT"
        )
        assert not ok and "Last-Modified" in reason

    def test_unvalidated(self, tmp_path: Path) -> None:
        state = PartState(part_path=tmp_path / "f.part", url="u", size=10, plan=[(0, 10)])
        ok, _ = state.matches_remote(size=10, etag=None, last_modified=None)
        assert not ok
        ok, _ = state.matches_remote(size=10, etag=None, last_modified=None, allow_unvalidated=True)
        assert ok

    def test_open_creates_state(self, tmp_path: Path) -> None:
        part = tmp_path / "f.part"
        part.write_bytes(b"")
        state, resumed = open_part_state(
            part,
            url="https://example.com/f",
            size=10,
            etag='"x"',
            last_modified=None,
            plan=[(0, 10)],
            backend="pycurl",
            resume=True,
        )
        assert not resumed
        assert state.size == 10

    def test_open_resumes(self, tmp_path: Path) -> None:
        part = tmp_path / "f.part"
        part.write_bytes(b"\0" * 10)
        state, _ = open_part_state(
            part,
            url="https://example.com/f",
            size=10,
            etag='"x"',
            last_modified=None,
            plan=[(0, 10)],
            backend="pycurl",
            resume=False,
        )
        state.mark(0, 4)
        state.save()

        state2, resumed = open_part_state(
            part,
            url="https://example.com/f",
            size=10,
            etag='"x"',
            last_modified=None,
            plan=[(0, 10)],
            backend="pycurl",
            resume=True,
        )
        assert resumed
        assert state2.done_bytes() == 4

    def test_open_restarts_on_etag_change(self, tmp_path: Path) -> None:
        part = tmp_path / "f.part"
        part.write_bytes(b"\0" * 10)
        state, _ = open_part_state(
            part,
            url="https://example.com/f",
            size=10,
            etag='"x"',
            last_modified=None,
            plan=[(0, 10)],
            backend="pycurl",
            resume=False,
        )
        state.mark(0, 4)
        state.save()
        state2, resumed = open_part_state(
            part,
            url="https://example.com/f",
            size=10,
            etag='"y"',
            last_modified=None,
            plan=[(0, 10)],
            backend="pycurl",
            resume=True,
        )
        assert not resumed
        assert state2.done_bytes() == 0

    def test_open_restarts_on_url_change(self, tmp_path: Path) -> None:
        part = tmp_path / "f.part"
        part.write_bytes(b"\0" * 10)
        state, _ = open_part_state(
            part,
            url="https://example.com/one",
            size=10,
            etag='"x"',
            last_modified=None,
            plan=[(0, 10)],
            backend="pycurl",
            resume=False,
        )
        state.mark(0, 4)
        state.save()
        _, resumed = open_part_state(
            part,
            url="https://example.com/two",
            size=10,
            etag='"x"',
            last_modified=None,
            plan=[(0, 10)],
            backend="pycurl",
            resume=True,
        )
        assert not resumed

    def test_corrupt_state_is_discarded(self, tmp_path: Path) -> None:
        part = tmp_path / "f.part"
        part.write_bytes(b"\0" * 10)
        bad = PartState.state_file_for(part)
        bad.write_text("{not json")
        state, resumed = open_part_state(
            part,
            url="https://example.com/f",
            size=10,
            etag='"x"',
            last_modified=None,
            plan=[(0, 10)],
            backend="pycurl",
            resume=True,
        )
        assert not resumed
        assert state.done_bytes() == 0
        assert not bad.exists()


def test_plan_changes_on_resume(tmp_path: Path) -> None:
    """Coverage tracking survives a different --connections value."""
    part = tmp_path / "f.part"
    part.write_bytes(b"\0" * 1000)
    state, _ = open_part_state(
        part,
        url="u",
        size=1000,
        etag='"x"',
        last_modified=None,
        plan=[(0, 500), (500, 1000)],
        backend="pycurl",
        resume=False,
    )
    state.mark(0, 500)
    state.save()
    state2, resumed = open_part_state(
        part,
        url="u",
        size=1000,
        etag='"x"',
        last_modified=None,
        plan=[(0, 250), (250, 500), (500, 750), (750, 1000)],
        backend="pycurl",
        resume=True,
    )
    assert resumed
    assert state2.missing() == [(500, 1000)]


def test_state_without_part_file(tmp_path: Path) -> None:
    part = tmp_path / "f.part"
    state = PartState(part_path=part, url="u", size=10, plan=[(0, 10)])
    state.mark(0, 10)
    state.save()
    _, resumed = open_part_state(
        part,
        url="u",
        size=10,
        etag=None,
        last_modified=None,
        plan=[(0, 10)],
        backend="pycurl",
        resume=True,
    )
    assert not resumed
