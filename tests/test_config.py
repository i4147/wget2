from __future__ import annotations

import logging
from pathlib import Path

import pytest

from wget2.config import (
    env_overrides,
    flatten_config,
    load_config,
    load_config_file,
    normalize_key,
    prescan_config_paths,
    resolve_defaults,
)
from wget2.errors import ConfigError


class TestToml:
    def test_load_sections(self, tmp_path: Path) -> None:
        cfg = tmp_path / "wget2.toml"
        cfg.write_text("""
[download]
backend = "httpx"
connections = 4
retry = 2
retry_delay = 0.5

[output]
dir = "downloads"
no_clobber = true
max_filesize = "10M"

[http]
header = ["X-Test: 1", "X-Other: 2"]
user_agent = "custom/1.0"
""")
        data = load_config_file(cfg)
        assert data["backend"] == "httpx"
        assert data["connections"] == 4
        assert data["retry_delay"] == 0.5
        assert data["output_dir"] == "downloads"
        assert data["no_clobber"] is True
        assert data["max_filesize"] == 10 * 1024 * 1024
        assert data["header"] == ["X-Test: 1", "X-Other: 2"]
        assert data["user_agent"] == "custom/1.0"

    def test_invalid_toml(self, tmp_path: Path) -> None:
        cfg = tmp_path / "bad.toml"
        cfg.write_text("this is not = = toml")
        with pytest.raises(ConfigError):
            load_config_file(cfg)

    def test_missing_file(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError):
            load_config_file(tmp_path / "nope.toml")

    def test_yaml(self, tmp_path: Path) -> None:
        pytest.importorskip("yaml")
        cfg = tmp_path / "wget2.yaml"
        cfg.write_text("download:\n  connections: 2\n  backend: requests\n")
        data = load_config_file(cfg)
        assert data["connections"] == 2
        assert data["backend"] == "requests"

    def test_multiple_files_merge(self, tmp_path: Path) -> None:
        one = tmp_path / "one.toml"
        one.write_text("[download]\nconnections = 2\nretry = 9\n")
        two = tmp_path / "two.toml"
        two.write_text("[download]\nconnections = 7\n")
        merged = load_config([one, two])
        assert merged["connections"] == 7
        assert merged["retry"] == 9


class TestFlatten:
    def test_aliases(self) -> None:
        flat = flatten_config({"continue": True, "out": "dir", "retries": 3})
        assert flat["continue_"] is True
        assert flat["output_dir"] == "dir"
        assert flat["retry"] == 3

    def test_normalize(self) -> None:
        assert normalize_key("max-per-host") == "max_per_host"
        assert normalize_key("no-clobber") == "no_clobber"

    def test_plural_key(self) -> None:
        assert flatten_config({"headers": ["A: b"]})["header"] == ["A: b"]

    def test_unknown_key_warns(self) -> None:
        # the wget2 logger does not propagate (it owns its handlers), so attach
        # a collector directly instead of relying on caplog/root propagation.
        records: list[logging.LogRecord] = []

        class Collector(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                records.append(record)

        logger = logging.getLogger("wget2.config")
        handler = Collector()
        logger.addHandler(handler)
        try:
            flat = flatten_config({"totally_unknown": 1})
        finally:
            logger.removeHandler(handler)
        assert flat == {}
        assert any("unknown config key" in record.getMessage() for record in records)

    def test_bad_int(self) -> None:
        with pytest.raises(ConfigError):
            flatten_config({"connections": "many"})


class TestEnv:
    def test_overrides(self) -> None:
        env = {
            "WGET2_BACKEND": "requests",
            "WGET2_CONNECTIONS": "3",
            "WGET2_NO_CLOBBER": "true",
            "WGET2_MAX_FILESIZE": "1M",
            "UNRELATED": "x",
        }
        data = env_overrides(env)
        assert data["backend"] == "requests"
        assert data["connections"] == 3
        assert data["no_clobber"] is True
        assert data["max_filesize"] == 1024 * 1024
        assert "UNRELATED" not in data

    def test_env_beats_config(self, tmp_path: Path, monkeypatch) -> None:
        cfg = tmp_path / "wget2.toml"
        cfg.write_text("[download]\nconnections = 2\n")
        monkeypatch.setenv("WGET2_CONNECTIONS", "9")
        defaults = resolve_defaults([cfg])
        assert defaults["connections"] == 9


class TestPrescan:
    def test_long_option(self) -> None:
        assert prescan_config_paths(["--config", "a.toml", "url"]) == [Path("a.toml")]

    def test_equals_form(self) -> None:
        assert prescan_config_paths(["--config=b.toml"]) == [Path("b.toml")]

    def test_env(self, monkeypatch) -> None:
        monkeypatch.setenv("WGET2_CONFIG", "c.toml")
        assert Path("c.toml") in prescan_config_paths([])

    def test_none(self, monkeypatch) -> None:
        monkeypatch.delenv("WGET2_CONFIG", raising=False)
        assert prescan_config_paths(["https://example.com"]) == []
