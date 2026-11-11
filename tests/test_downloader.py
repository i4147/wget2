"""Engine-level behaviour that is awkward to reach through the CLI."""

from __future__ import annotations

from pathlib import Path

import pytest

from wget2.backends.base import DownloadBackend, ResponseInfo, Sink
from wget2.backends.registry import REGISTRY
from wget2.downloader import DownloadContext, FileDownloader, InterruptFlag
from wget2.errors import TransportError
from wget2.output import OutputOptions
from wget2.progress import NullReporter
from wget2.request import RequestConfig
from wget2.settings import Settings

pytestmark = pytest.mark.network


def build_context(outdir: Path, **overrides) -> DownloadContext:
    overrides.setdefault("backend", "pycurl")
    settings = Settings(
        output=OutputOptions(output_dir=outdir),
        progress_mode="none",
        retry=0,
        request=RequestConfig(timeout=10.0, connect_timeout=5.0),
        **overrides,
    )
    return DownloadContext.build(settings, REGISTRY, NullReporter())


class TestInterrupt:
    def test_interrupt_before_start(self, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        context = build_context(outdir)
        try:
            context.interrupt.set()
            result = FileDownloader(context).run(url)
        finally:
            context.close()
        assert result.exit_code == 130
        assert result.error_kind == "interrupted"
        assert not (outdir / "file.bin").exists()

    def test_interrupt_flag_api(self) -> None:
        from wget2.errors import Interrupted

        flag = InterruptFlag()
        flag.check()
        flag.set()
        assert flag.is_set()
        with pytest.raises(Interrupted):
            flag.check()
        flag.clear()
        flag.check()


class TestPartialPolicy:
    def _failing_url(self, server, payload) -> str:
        return server.add("cut.bin", payload, allow_ranges=False, truncate_at=512)

    def test_keep_partial_default(self, server, outdir, payload) -> None:
        url = self._failing_url(server, payload)
        context = build_context(outdir)
        try:
            result = FileDownloader(context).run(url)
        finally:
            context.close()
        assert result.status == "failed"
        assert (outdir / "cut.bin.part").exists()

    def test_remove_on_error(self, server, outdir, payload) -> None:
        url = self._failing_url(server, payload)
        context = build_context(outdir, remove_on_error=True)
        try:
            result = FileDownloader(context).run(url)
        finally:
            context.close()
        assert result.status == "failed"
        assert not (outdir / "cut.bin.part").exists()
        assert not (outdir / "cut.bin.part.json").exists()

    def test_no_keep_partial(self, server, outdir, payload) -> None:
        url = self._failing_url(server, payload)
        context = build_context(outdir, keep_partial=False)
        try:
            FileDownloader(context).run(url)
        finally:
            context.close()
        assert not (outdir / "cut.bin.part").exists()


class TestBackendFailover:
    def test_falls_back_to_next_backend(self, server, outdir, payload, monkeypatch) -> None:
        url = server.add("file.bin", payload)

        class BrokenBackend(DownloadBackend):
            name = "broken"
            requires = ()

            def _request(
                self,
                url: str,
                method: str,
                headers: list[tuple[str, str]],
                sink: Sink | None,
                *,
                allow_status: frozenset[int] = frozenset(),
            ) -> ResponseInfo:
                raise TransportError("backend is broken", url=url)

        monkeypatch.setitem(REGISTRY._backends, "broken", BrokenBackend)  # noqa: SLF001
        context = build_context(outdir, backend="broken", backend_auto_fallback=True)
        try:
            result = FileDownloader(context).run(url)
        finally:
            context.close()
        assert result.status == "downloaded", result.error
        assert result.backend != "broken"
        assert (outdir / "file.bin").read_bytes() == payload


class TestAdaptiveConnections:
    @pytest.mark.parametrize(
        ("size", "expected"),
        [(None, 1), (0, 1), (100, 1), (2 * 1024 * 1024, 8), (1024 * 1024 * 1024, 8)],
    )
    def test_effective_connections(self, size, expected) -> None:
        settings = Settings(connections=8)
        assert settings.effective_connections(size) == expected

    def test_capped_for_tiny_files(self) -> None:
        settings = Settings(connections=32)
        assert settings.effective_connections(200 * 1024) == 1

    def test_hard_cap(self) -> None:
        settings = Settings(connections=100)
        assert settings.effective_connections(100 * 1024 * 1024) <= 32


class TestLimiters:
    def test_global_limiter_created(self) -> None:
        settings = Settings(rate_limit=1024.0)
        assert settings.global_limiter() is not None
        assert settings.global_limiter().enabled

    def test_no_limiter_by_default(self) -> None:
        assert Settings().global_limiter() is None
        assert Settings().connection_limiter() is None


class TestRetryPolicySettings:
    def test_policy_from_settings(self) -> None:
        settings = Settings(retry=3, retry_delay=0.5, retry_backoff=3.0, retry_http=frozenset({503}))
        policy = settings.retry_policy()
        assert policy.attempts == 4
        assert policy.delay == 0.5
        assert policy.backoff == 3.0
        assert policy.retry_status == frozenset({503})
