"""``--config`` file loading (TOML/YAML) plus ``WGET2_*`` environment overrides.

Precedence, highest first::

    command line  >  environment  >  config file  >  built-in default
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

from .errors import ConfigError
from .log import get_logger
from .util import parse_size

ENV_PREFIX = "WGET2_CONFIG"

#: every key accepted in a config file / environment variable
KNOWN_KEYS = frozenset(
    {
        "backend",
        "backend_args",
        "backend_auto_fallback",
        "connections",
        "max_per_host",
        "jobs",
        "max_active",
        "retry",
        "retry_delay",
        "retry_backoff",
        "retry_max_delay",
        "retry_jitter",
        "retry_http",
        "timeout",
        "connect_timeout",
        "max_redirects",
        "continue",
        "continue_",
        "allow_unvalidated_resume",
        "keep_partial",
        "remove_on_error",
        "check_disk",
        "checksum",
        "checksum_file",
        "user",
        "password",
        "bearer_token",
        "netrc",
        "netrc_file",
        "header",
        "user_agent",
        "referer",
        "cookie",
        "cookie_jar",
        "method",
        "data",
        "data_binary",
        "proxy",
        "no_proxy",
        "insecure",
        "ca_cert",
        "client_cert",
        "client_key",
        "http2",
        "output",
        "output_dir",
        "output_template",
        "template",
        "no_clobber",
        "overwrite",
        "timestamping",
        "unique",
        "content_disposition",
        "manifest",
        "progress",
        "no_progress",
        "quiet",
        "verbose",
        "json",
        "log_file",
        "log_level",
        "log_json",
        "max_filesize",
        "rate_limit",
        "rate_limit_per_connection",
        "buffer_threshold",
        "recursive",
        "mirror",
        "level",
        "spider",
        "dry_run",
        "accept",
        "reject",
        "include",
        "exclude",
        "base_url",
        "allow_unsafe_schemes",
        "input_format",
        "mirror_list",
        "lock_file",
        "no_lock",
        "jobs_per_host",
    }
)

KEY_ALIASES = {
    "continue": "continue_",
    "resume": "continue_",
    "retries": "retry",
    "retries_delay": "retry_delay",
    "out": "output_dir",
    "dir": "output_dir",
    "output-dir": "output_dir",
    "output-template": "output_template",
    "no-clobber": "no_clobber",
    "user-agent": "user_agent",
    "rate-limit": "rate_limit",
    "max-filesize": "max_filesize",
    "log-level": "log_level",
    "log-file": "log_file",
    "ca-cert": "ca_cert",
    "client-cert": "client_cert",
    "client-key": "client_key",
    "bearer-token": "bearer_token",
}

#: keys whose value is a size ("10M") or a number of bytes
SIZE_KEYS = frozenset({"max_filesize", "rate_limit", "rate_limit_per_connection", "buffer_threshold"})
LIST_KEYS = frozenset(
    {"header", "checksum", "accept", "reject", "include", "exclude", "backend_args", "mirror_list"}
)
BOOL_KEYS = frozenset(
    {
        "no_clobber",
        "overwrite",
        "timestamping",
        "unique",
        "manifest",
        "quiet",
        "verbose",
        "json",
        "no_progress",
        "recursive",
        "mirror",
        "spider",
        "dry_run",
        "insecure",
        "netrc",
        "no_proxy",
        "http2",
        "keep_partial",
        "remove_on_error",
        "check_disk",
        "allow_unsafe_schemes",
        "no_lock",
        "backend_auto_fallback",
        "allow_unvalidated_resume",
        "log_json",
        "content_disposition",
    }
)
INT_KEYS = frozenset({"connections", "max_per_host", "jobs", "max_active", "retry", "max_redirects", "level"})
FLOAT_KEYS = frozenset(
    {"retry_delay", "retry_backoff", "retry_max_delay", "retry_jitter", "timeout", "connect_timeout"}
)


def normalize_key(key: str) -> str:
    lowered = str(key).strip().replace("-", "_").lower()
    return KEY_ALIASES.get(lowered, lowered)


def prescan_config_paths(argv: list[str] | None = None) -> list[Path]:
    """Find ``--config`` before Typer parses (defaults depend on it)."""
    args = list(sys.argv[1:] if argv is None else argv)
    paths: list[Path] = []
    index = 0
    while index < len(args):
        arg = args[index]
        if arg == "--config" and index + 1 < len(args):
            paths.append(Path(args[index + 1]))
            index += 2
            continue
        if arg.startswith("--config="):
            paths.append(Path(arg.split("=", 1)[1]))
        index += 1
    env_path = os.environ.get(ENV_PREFIX)
    if env_path:
        paths.extend(Path(item) for item in env_path.split(os.pathsep) if item)
    return paths


def load_config_file(path: Path) -> dict[str, Any]:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ConfigError(f"config file not found: {path}") from exc
    except OSError as exc:
        raise ConfigError(f"cannot read config file {path}: {exc}") from exc
    suffix = path.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        try:
            import yaml
        except ImportError as exc:
            raise ConfigError("YAML config requires PyYAML (pip install 'wget2[yaml]')") from exc
        try:
            data = yaml.safe_load(text) or {}
        except Exception as exc:  # pragma: no cover - depends on PyYAML
            raise ConfigError(f"invalid YAML in {path}: {exc}") from exc
    else:
        try:
            import tomllib
        except ImportError:  # pragma: no cover - py<3.11
            try:
                import tomli as tomllib
            except ImportError as exc:
                raise ConfigError("TOML config requires Python 3.11+ or tomli") from exc
        try:
            data = tomllib.loads(text)
        except Exception as exc:
            raise ConfigError(f"invalid TOML in {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"config root must be a table/mapping: {path}")
    return flatten_config(data, source=str(path))


def flatten_config(data: dict[str, Any], *, source: str = "config") -> dict[str, Any]:
    """Flatten sectioned config into option names, warning on unknown keys."""
    logger = get_logger("config")
    flat: dict[str, Any] = {}
    for key, value in data.items():
        if isinstance(value, dict):
            flat.update(flatten_config(dict(value), source=source))
            continue
        normalized = normalize_key(key)
        canonical = _canonical(normalized)
        if canonical is None:
            logger.warning("unknown config key ignored", key=key, source=source)
            continue
        flat[canonical] = coerce_value(canonical, value)
    return flat


def _canonical(key: str) -> str | None:
    if key in KNOWN_KEYS:
        return key
    if key.endswith("s") and key[:-1] in KNOWN_KEYS:  # "headers" -> "header"
        return key[:-1]
    return None


def coerce_value(key: str, value: Any) -> Any:
    if key in SIZE_KEYS and isinstance(value, str):
        return parse_size(value)
    if key in BOOL_KEYS:
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)
    if key in INT_KEYS and not isinstance(value, bool):
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"config key '{key}' expects an integer, got {value!r}") from exc
    if key in FLOAT_KEYS:
        try:
            return float(value)
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"config key '{key}' expects a number, got {value!r}") from exc
    if key in LIST_KEYS:
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return list(value) if isinstance(value, (list, tuple)) else [value]
    return value


def load_config(paths: list[Path] | None) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    logger = get_logger("config")
    for path in paths or []:
        data = load_config_file(path)
        merged.update(data)
        logger.debug("config file loaded", path=str(path), keys=len(data))
    return merged


def env_overrides(environ: dict[str, str] | None = None) -> dict[str, Any]:
    """Read ``WGET2_*`` variables into option values."""
    source = dict(os.environ if environ is None else environ)
    out: dict[str, Any] = {}
    for raw_key, value in source.items():
        if not raw_key.startswith("WGET2_") or raw_key == ENV_PREFIX:
            continue
        key = normalize_key(raw_key[len("WGET2_") :])
        canonical = _canonical(key)
        if canonical is None:
            continue
        try:
            out[canonical] = coerce_value(canonical, value)
        except ConfigError:
            get_logger("config").warning("ignoring invalid environment value", key=raw_key)
    return out


def resolve_defaults(paths: list[Path] | None = None) -> dict[str, Any]:
    """Config file values overridden by environment variables."""
    defaults = load_config(paths)
    defaults.update(env_overrides())
    return defaults
