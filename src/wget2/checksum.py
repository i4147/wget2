"""Checksum parsing and verification (``--checksum`` / ``--checksum-file``)."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

from .errors import ChecksumError, ConfigError, UsageError
from .log import get_logger
from .util import human_size

SUPPORTED_ALGORITHMS = frozenset(
    {"md5", "sha1", "sha224", "sha256", "sha384", "sha512", "blake2b", "sha3_256"}
)
DEFAULT_ALGORITHM = "sha256"
_HEX_RE = re.compile(r"^[0-9a-fA-F]+$")
_CHUNK = 1024 * 1024


@dataclass(frozen=True)
class ChecksumSpec:
    algorithm: str
    expected: str
    source: str = "cli"

    def __str__(self) -> str:
        return f"{self.algorithm}:{self.expected}"


def parse_checksum(value: str) -> ChecksumSpec:
    """Parse ``sha256:<hex>``, ``sha256=<hex>`` or a bare hex digest."""
    text = (value or "").strip()
    if not text:
        raise UsageError("empty --checksum value")
    algorithm = DEFAULT_ALGORITHM
    digest = text
    for separator in (":", "="):
        if separator in text:
            head, _, tail = text.partition(separator)
            if head.strip().lower() in SUPPORTED_ALGORITHMS:
                algorithm, digest = head.strip().lower(), tail.strip()
                break
    digest = digest.strip().lower()
    if not _HEX_RE.match(digest):
        raise UsageError(f"invalid checksum digest (expected hex): {value!r}")
    try:
        expected_length = hashlib.new(algorithm).digest_size * 2
    except ValueError as exc:  # pragma: no cover - guarded above
        raise UsageError(f"unsupported checksum algorithm: {algorithm}") from exc
    if len(digest) != expected_length:
        raise UsageError(f"{algorithm} digest must be {expected_length} hex chars, got {len(digest)}")
    return ChecksumSpec(algorithm=algorithm, expected=digest)


def parse_checksums(values: list[str] | None) -> list[ChecksumSpec]:
    return [parse_checksum(v) for v in (values or []) if v and v.strip()]


def load_checksum_file(path: Path) -> dict[str, list[ChecksumSpec]]:
    """Read ``sha256sum``-style files (``<hex>  <name>``) into a lookup table."""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        raise ConfigError(f"cannot read checksum file {path}: {exc}") from exc
    table: dict[str, list[ChecksumSpec]] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("SHA256 (") or line.startswith("SHA1 (") or line.startswith("MD5 ("):
            # BSD format: ALGO (name) = hex
            match = re.match(r"(?i)(\w+)\s*\((.+)\)\s*=\s*([0-9a-fA-F]+)", line)
            if not match:
                continue
            algorithm, name, digest = match.groups()
            table.setdefault(Path(name).name, []).append(
                ChecksumSpec(algorithm.lower(), digest.lower(), source=str(path))
            )
            continue
        parts = line.replace("\t", " ").split(None, 1)
        if len(parts) != 2:
            continue
        digest, name = parts
        name = name.lstrip("*").strip()
        try:
            spec = parse_checksum(f"{_guess_algorithm(digest)}:{digest}")
        except UsageError:
            continue
        table.setdefault(Path(name).name, []).append(
            ChecksumSpec(spec.algorithm, spec.expected, source=str(path))
        )
    get_logger("checksum").debug("checksum file loaded", path=str(path), entries=len(table))
    return table


def _guess_algorithm(digest: str) -> str:
    lengths = {32: "md5", 40: "sha1", 56: "sha224", 64: "sha256", 96: "sha384", 128: "sha512"}
    return lengths.get(len(digest.strip()), DEFAULT_ALGORITHM)


def specs_for(filename: str, table: dict[str, list[ChecksumSpec]]) -> list[ChecksumSpec]:
    if not table:
        return []
    candidates = [filename, Path(filename).name, f"*{Path(filename).name}"]
    for key in candidates:
        if key in table:
            return table[key]
    # glob-ish entries such as "*"
    for key, specs in table.items():
        if key == "*":
            return specs
    get_logger("checksum").warning("no checksum entry for file", filename=filename)
    return []


def hash_file(path: Path, algorithm: str = DEFAULT_ALGORITHM) -> str:
    try:
        hasher = hashlib.new(algorithm)
    except ValueError as exc:
        raise UsageError(f"unsupported checksum algorithm: {algorithm}") from exc
    try:
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(_CHUNK)
                if not chunk:
                    break
                hasher.update(chunk)
    except OSError as exc:
        raise ChecksumError(f"cannot read {path} for checksum: {exc}") from exc
    return hasher.hexdigest()


def verify_file(path: Path, specs: list[ChecksumSpec]) -> dict[str, str]:
    """Verify ``path`` against every spec; returns the computed digests."""
    digests: dict[str, str] = {}
    size = path.stat().st_size if path.exists() else 0
    for spec in specs:
        if spec.algorithm not in digests:
            digests[spec.algorithm] = hash_file(path, spec.algorithm)
        actual = digests[spec.algorithm]
        if actual != spec.expected:
            raise ChecksumError(
                f"{spec.algorithm} mismatch for {path.name} ({human_size(size)}): "
                f"expected {spec.expected}, got {actual}",
                details={"algorithm": spec.algorithm, "expected": spec.expected, "actual": actual},
            )
        get_logger("checksum").info(
            "checksum verified", filename=path.name, algorithm=spec.algorithm, digest=actual
        )
    return digests
