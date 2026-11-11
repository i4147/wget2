"""Backend conformance tests against the local server."""

from __future__ import annotations

import threading

import pytest

from wget2.backends.base import StopStream
from wget2.backends.registry import REGISTRY
from wget2.errors import BackendUnavailableError, HTTPStatusError, TransportError
from wget2.request import RequestConfig

AVAILABLE = [name for name in ("pycurl", "httpx", "requests", "httpx_async") if REGISTRY.get(name).available()]
pytestmark = pytest.mark.network


@pytest.fixture(params=AVAILABLE)
def backend(request):
    instance = REGISTRY.create(request.param, RequestConfig(timeout=10.0, connect_timeout=5.0))
    yield instance
    instance.close()


def test_registry_lists_backends() -> None:
    names = REGISTRY.names()
    assert {"pycurl", "httpx", "requests", "httpx_async"} <= set(names)
    infos = {info.name: info for info in REGISTRY.describe()}
    assert infos["pycurl"].source == "builtin"


def test_unknown_backend() -> None:
    with pytest.raises(Exception):
        REGISTRY.create("nope", RequestConfig())


def test_auto_resolution() -> None:
    assert REGISTRY.canonical("auto") == "auto"
    assert REGISTRY.resolve_auto() in REGISTRY.names()


def test_aliases() -> None:
    assert REGISTRY.canonical("async") == "httpx_async"
    assert REGISTRY.canonical("curl") == "pycurl"


def test_fallback_chain() -> None:
    chain = REGISTRY.fallback_chain("auto")
    assert chain and chain[0] == REGISTRY.resolve_auto()
    assert len(chain) == len(set(chain))


class TestProbe:
    def test_size_and_validators(self, backend, server, payload) -> None:
        url = server.add("file.bin", payload)
        probe = backend.probe(url)
        assert probe.size == len(payload)
        assert probe.accept_ranges is True
        assert probe.etag
        assert probe.last_modified
        assert probe.suggested_filename == "file.bin"

    def test_no_range_support(self, backend, server, payload) -> None:
        url = server.add("plain.bin", payload, allow_ranges=False)
        probe = backend.probe(url)
        assert probe.size == len(payload)
        assert probe.accept_ranges is False

    def test_content_disposition(self, backend, server, payload) -> None:
        url = server.add("cd.bin", payload, content_disposition='attachment; filename="nice name.bin"')
        assert backend.probe(url).suggested_filename == "nice name.bin"

    def test_404(self, backend, server) -> None:
        with pytest.raises(HTTPStatusError) as excinfo:
            backend.probe(server.url("missing.bin"))
        assert excinfo.value.status_code == 404
        assert not excinfo.value.retryable

    def test_redirect_followed(self, backend, server, payload) -> None:
        server.add("target.bin", payload)
        probe = backend.probe(server.url("redirect/target.bin"))
        assert probe.size == len(payload)
        assert probe.final_url.endswith("target.bin")


class TestStreaming:
    def test_full_body(self, backend, server, payload) -> None:
        url = server.add("file.bin", payload)
        chunks: list[bytes] = []
        info = backend.stream_range(url, None, None, chunks.append)
        assert info.status == 200
        assert b"".join(chunks) == payload

    def test_range(self, backend, server, payload) -> None:
        url = server.add("file.bin", payload)
        chunks: list[bytes] = []
        info = backend.stream_range(url, 100, 199, chunks.append)
        assert info.status == 206
        assert b"".join(chunks) == payload[100:200]
        assert info.header("content-range") == f"bytes 100-199/{len(payload)}"

    def test_tail_range(self, backend, server, payload) -> None:
        url = server.add("file.bin", payload)
        chunks: list[bytes] = []
        info = backend.stream_range(url, len(payload) - 10, len(payload) - 1, chunks.append)
        assert info.status == 206
        assert b"".join(chunks) == payload[-10:]

    def test_stop_stream_aborts_early(self, backend, server, payload) -> None:
        url = server.add("file.bin", payload)
        received: list[int] = []

        def sink(chunk: bytes) -> None:
            received.append(len(chunk))
            raise StopStream

        backend.stream_range(url, None, None, sink)
        assert sum(received) < len(payload)

    def test_error_status_does_not_reach_sink(self, backend, server) -> None:
        received: list[bytes] = []
        with pytest.raises(HTTPStatusError):
            backend.stream_range(server.url("nope.bin"), None, None, received.append)
        assert received == []

    def test_basic_auth(self, server, payload) -> None:
        server.add("secret.bin", payload)
        server.auth["secret.bin"] = ("alice", "s3cret")
        config = RequestConfig(user="alice", password="s3cret", timeout=10.0)
        backend = REGISTRY.create("requests", config)
        try:
            chunks: list[bytes] = []
            backend.stream_range(server.url("auth/secret.bin"), None, None, chunks.append)
            assert b"".join(chunks) == payload
        finally:
            backend.close()

    def test_bad_credentials(self, server, payload) -> None:
        server.add("secret.bin", payload)
        server.auth["secret.bin"] = ("alice", "s3cret")
        backend = REGISTRY.create("httpx", RequestConfig(user="alice", password="wrong", timeout=10.0))
        try:
            with pytest.raises(HTTPStatusError) as excinfo:
                backend.stream_range(server.url("auth/secret.bin"), None, None, lambda _c: None)
            assert excinfo.value.status_code == 401
        finally:
            backend.close()

    def test_bearer_token(self, server, payload) -> None:
        server.add("t.bin", payload)
        server.bearer["t.bin"] = "tok-123"
        backend = REGISTRY.create("pycurl", RequestConfig(bearer_token="tok-123", timeout=10.0))
        try:
            chunks: list[bytes] = []
            backend.stream_range(server.url("bearer/t.bin"), None, None, chunks.append)
            assert b"".join(chunks) == payload
        finally:
            backend.close()

    def test_custom_headers(self, server, payload) -> None:
        server.add("h.bin", payload)
        config = RequestConfig(
            headers=(("X-Test", "value"),),
            user_agent="wget2-test/1.0",
            referer="https://example.com/ref",
            timeout=10.0,
        )
        backend = REGISTRY.create("requests", config)
        try:
            chunks: list[bytes] = []
            backend.stream_range(server.url("echo"), None, None, chunks.append)
            import json

            echoed = json.loads(b"".join(chunks)).get("headers", {})
            lowered = {key.lower(): value for key, value in echoed.items()}
            assert lowered.get("x-test") == "value"
            assert lowered.get("user-agent") == "wget2-test/1.0"
            assert lowered.get("referer") == "https://example.com/ref"
        finally:
            backend.close()

    def test_connection_refused(self) -> None:
        backend = REGISTRY.create("requests", RequestConfig(timeout=1.0, connect_timeout=1.0))
        try:
            with pytest.raises(TransportError):
                backend.stream_range("http://127.0.0.1:1/x", None, None, lambda _c: None)
        finally:
            backend.close()


class TestAsyncBackend:
    def test_single_event_loop(self, server, payload) -> None:
        """All ranges of httpx_async must share one event loop / thread."""
        url = server.add("file.bin", payload)
        backend = REGISTRY.create("httpx_async", RequestConfig(timeout=10.0))
        threads: set[int] = set()

        def sink(chunk: bytes) -> None:
            if chunk:
                threads.add(threading.get_ident())

        try:
            backend.stream_range(url, 0, 1023, sink)
            backend.stream_range(url, 1024, 2047, sink)
            backend.stream_range(url, 2048, 3071, sink)
        finally:
            backend.close()
        assert len(threads) == 1
        assert threading.get_ident() not in threads

    def test_available_flag(self) -> None:
        assert REGISTRY.get("httpx_async").supports_async is True


def test_unavailable_backend_message(monkeypatch) -> None:
    from wget2.backends.pycurl_backend import PycurlBackend

    monkeypatch.setattr(PycurlBackend, "available", classmethod(lambda cls: False))
    monkeypatch.setattr(PycurlBackend, "availability_reason", classmethod(lambda cls: "missing optional dependency"))
    with pytest.raises(BackendUnavailableError):
        REGISTRY.create("pycurl", RequestConfig())


class TestPlugins:
    def test_entry_point_discovery(self, monkeypatch) -> None:
        """Third-party backends are found through wget2.backends entry points."""
        from importlib import metadata as metadata_module

        from wget2.backends.base import DownloadBackend, ResponseInfo, Sink
        from wget2.backends.registry import BackendRegistry

        class DemoBackend(DownloadBackend):
            name = "demo"
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
                if sink is not None:
                    sink(b"hello")
                return ResponseInfo(200, url, {"content-length": "5"})

        class FakeEntryPoint:
            name = "demo"
            value = "demo_pkg:DemoBackend"
            dist = type("D", (), {"name": "demo-pkg"})()

            def load(self):
                return DemoBackend

        def fake_entry_points(group=None):  # noqa: ARG001
            return [FakeEntryPoint()]

        monkeypatch.setattr(metadata_module, "entry_points", fake_entry_points)
        registry = BackendRegistry()
        found = registry.load_plugins()
        assert "demo" in found
        assert registry.canonical("demo") == "demo"
        info = {item.name: item for item in registry.describe()}["demo"]
        assert info.source.startswith("plugin:")

    def test_broken_plugin_is_ignored(self, monkeypatch) -> None:
        from importlib import metadata as metadata_module

        from wget2.backends.registry import BackendRegistry

        class Exploding:
            name = "boom"
            value = "boom:x"
            dist = None

            def load(self):
                raise ImportError("nope")

        monkeypatch.setattr(
            metadata_module,
            "entry_points",
            lambda group=None: [Exploding()],  # noqa: ARG005
        )
        registry = BackendRegistry()
        assert registry.load_plugins() == {}
        assert "boom" not in registry.names()

    def test_plugin_not_a_backend(self, monkeypatch) -> None:
        from importlib import metadata as metadata_module

        from wget2.backends.registry import BackendRegistry

        class NotABackend:
            name = "weird"
            value = "weird:x"
            dist = None

            def load(self):
                return object()

        monkeypatch.setattr(
            metadata_module,
            "entry_points",
            lambda group=None: [NotABackend()],  # noqa: ARG005
        )
        registry = BackendRegistry()
        assert registry.load_plugins() == {}


class TestBackendArgs:
    def test_parse(self) -> None:
        from wget2.backends.registry import parse_backend_args

        parsed = parse_backend_args(["max_connections=64,keepalive=8", "flag=true", "name=abc"])
        assert parsed == {"max_connections": 64, "keepalive": 8, "flag": True, "name": "abc"}

    def test_invalid(self) -> None:
        from wget2.backends.registry import parse_backend_args
        from wget2.errors import ConfigError

        with pytest.raises(ConfigError):
            parse_backend_args(["nope"])

    def test_passed_to_backend(self) -> None:
        backend = REGISTRY.create("httpx_async", RequestConfig(), {"max_connections": 7})
        try:
            assert backend.extra["max_connections"] == 7
        finally:
            backend.close()


class TestSinkFailures:
    def test_pycurl_propagates_sink_exception(self, server, payload) -> None:
        """A failing sink must surface as itself, not as a libcurl error."""
        from wget2.errors import Interrupted

        url = server.add("file.bin", payload)
        backend = REGISTRY.create("pycurl", RequestConfig(timeout=10.0))
        try:
            with pytest.raises(Interrupted):
                backend.stream_range(url, None, None, _explode_after_first_chunk)
        finally:
            backend.close()

    def test_httpx_propagates_sink_exception(self, server, payload) -> None:
        from wget2.errors import Interrupted

        url = server.add("file.bin", payload)
        backend = REGISTRY.create("httpx", RequestConfig(timeout=10.0))
        try:
            with pytest.raises(Interrupted):
                backend.stream_range(url, None, None, _explode_after_first_chunk)
        finally:
            backend.close()


def _explode_after_first_chunk(_chunk: bytes) -> None:
    from wget2.errors import Interrupted

    raise Interrupted("stop now")
