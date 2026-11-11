"""Progress reporters."""

from __future__ import annotations

import io

import pytest

from wget2.progress import (
    NullReporter,
    PlainReporter,
    ProgressReporter,
    RichReporter,
    make_reporter,
)


class TestNullReporter:
    def test_no_output(self) -> None:
        reporter = NullReporter()
        reporter.start(total_files=2)
        handle = reporter.add_file("x", 100)
        reporter.update(handle, 50)
        reporter.finish(handle)
        reporter.log("nothing")
        reporter.close()


class TestPlainReporter:
    def test_reports_progress(self) -> None:
        stream = io.StringIO()
        reporter = PlainReporter(stream, interval=0.0)
        reporter.start(total_files=1)
        handle = reporter.add_file("file.bin", 1000, already_done=100)
        reporter.update(handle, 400)
        reporter.finish(handle, status="done")
        text = stream.getvalue()
        assert "file.bin" in text
        assert "done" in text
        assert "%" in text

    def test_unknown_total(self) -> None:
        stream = io.StringIO()
        reporter = PlainReporter(stream, interval=0.0)
        handle = reporter.add_file("stream.bin", None)
        reporter.update(handle, 2048)
        assert "stream.bin" in stream.getvalue()

    def test_task_handles_are_unique(self) -> None:
        reporter = PlainReporter(io.StringIO())
        first = reporter.add_file("a", 10)
        second = reporter.add_file("b", 10)
        assert first.id != second.id

    def test_update_without_task_is_safe(self) -> None:
        reporter = PlainReporter(io.StringIO())
        reporter.update(None, 10)  # type: ignore[arg-type]
        reporter.finish(None)  # type: ignore[arg-type]


class TestRichReporter:
    def test_smoke(self) -> None:
        pytest.importorskip("rich")
        reporter = RichReporter(show_overall=True)
        reporter.start(total_files=2, total_bytes=200)
        first = reporter.add_file("a.bin", 100)
        second = reporter.add_file("b.bin", 100, already_done=50)
        reporter.update(first, 40)
        reporter.update(second, 10)
        reporter.finish(first)
        reporter.finish(second)
        reporter.log("hello")
        reporter.close()


class TestFactory:
    @pytest.mark.parametrize(
        ("mode", "expected"),
        [("none", NullReporter), ("plain", PlainReporter), ("rich", ProgressReporter)],
    )
    def test_modes(self, mode: str, expected: type) -> None:
        reporter = make_reporter(mode)
        assert isinstance(reporter, expected)
        reporter.close()

    def test_invalid_mode(self) -> None:
        with pytest.raises(ValueError):
            make_reporter("fancy")
