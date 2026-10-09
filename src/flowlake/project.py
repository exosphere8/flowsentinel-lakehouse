"""The dbt project that runs against a lake: the packaged project plus local configuration.

A deployment configures the lakehouse with an optional directory (``--config`` or
FLOWLAKE_CONFIG). Every file in it is optional:

``network_zones.csv``
    Replaces the default zones. Columns: ``zone,cidr,is_internal,description``. Add your own
    address ranges, including public ranges you own, so that direction (inbound, outbound,
    internal) is classified correctly. The most specific range wins.
``detection_allowlist.csv``
    Replaces the default allowlist. Columns: ``rule_id,src_ip,dst_ip,dst_port,reason``; ``*``
    matches anything. Use it for traffic that is legitimate but looks like a detection, for
    example a backup server's nightly upload.
``detections.yml``
    Overrides dbt vars such as detection thresholds, for example
    ``exfiltration_min_bytes_out: 2000000000``. Unknown names are an error.

Files are validated before anything runs, so a typo fails loudly instead of misclassifying
traffic. The packaged project and the overrides are copied to
``<lake>/.dbt/projects/<hash>/``; the hash covers every file, so each configuration gets its
own directory, created atomically and reused while nothing changes.
"""

from __future__ import annotations

import csv
import hashlib
import io
import ipaddress
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from flowlake.bronze import Lake

ZONE_COLUMNS = ("zone", "cidr", "is_internal", "description")
ALLOWLIST_COLUMNS = ("rule_id", "src_ip", "dst_ip", "dst_port", "reason")
VARS_FILE = "detections.yml"
# Directories in a dbt project that are outputs, never inputs.
_SKIPPED_DIRS = frozenset({"target", "logs", "dbt_packages"})


class ConfigError(ValueError):
    """The deployment configuration is invalid."""


@dataclass(frozen=True)
class ProjectSettings:
    project_dir: Path
    variables: dict[str, Any] = field(default_factory=dict)


def packaged_project_dir() -> Path:
    """The dbt project: FLOWLAKE_DBT_PROJECT, else ``transform/`` in a source checkout, else
    the copy installed inside the package."""
    configured = os.environ.get("FLOWLAKE_DBT_PROJECT")
    candidates = [Path(configured)] if configured else []
    here = Path(__file__).resolve().parent
    candidates += [here.parents[1] / "transform", here / "dbt_project"]
    for candidate in candidates:
        if (candidate / "dbt_project.yml").is_file():
            return candidate
    raise FileNotFoundError(
        "dbt project not found: reinstall the package, or set FLOWLAKE_DBT_PROJECT"
    )


def config_dir_from_env() -> Path | None:
    configured = os.environ.get("FLOWLAKE_CONFIG")
    return Path(configured) if configured else None


def prepare_project(lake: Lake, config_dir: Path | None = None) -> ProjectSettings:
    """Validate ``config_dir`` and return the project directory and dbt vars to run with."""
    source = packaged_project_dir()
    files = _project_files(source)
    overrides, variables = load_config(config_dir, source)
    files.update(overrides)

    digest = hashlib.sha256()
    for relative in sorted(files):
        content = files[relative]
        digest.update(relative.encode() + b"\0" + hashlib.sha256(content).digest())
    projects = lake.root / ".dbt" / "projects"
    target = projects / digest.hexdigest()[:20]
    if not (target / "dbt_project.yml").exists():
        projects.mkdir(parents=True, exist_ok=True)
        scratch = Path(tempfile.mkdtemp(prefix=".building-", dir=projects))
        try:
            for relative, content in files.items():
                path = scratch / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
            try:
                scratch.rename(target)  # atomic; fails if another process got there first
            except OSError:
                if not (target / "dbt_project.yml").exists():
                    raise
        finally:
            shutil.rmtree(scratch, ignore_errors=True)
    return ProjectSettings(project_dir=target, variables=variables)


def _project_files(source: Path) -> dict[str, bytes]:
    files = {}
    for path in sorted(source.rglob("*")):
        relative = path.relative_to(source)
        if relative.parts[0] in _SKIPPED_DIRS or not path.is_file():
            continue
        files[relative.as_posix()] = path.read_bytes()
    return files


def load_config(config_dir: Path | None, project: Path) -> tuple[dict[str, bytes], dict[str, Any]]:
    """Validated seed overrides (relative path to content) and dbt vars from ``config_dir``."""
    if config_dir is None:
        return {}, {}
    if not config_dir.is_dir():
        raise ConfigError(f"configuration directory not found: {config_dir}")
    overrides: dict[str, bytes] = {}
    zones = config_dir / "network_zones.csv"
    if zones.exists():
        overrides["seeds/network_zones.csv"] = _validated_csv(zones, ZONE_COLUMNS, _check_zone)
    allowlist = config_dir / "detection_allowlist.csv"
    if allowlist.exists():
        rules = _known_rules(project)
        overrides["seeds/detection_allowlist.csv"] = _validated_csv(
            allowlist, ALLOWLIST_COLUMNS, lambda row: _check_allow(row, rules)
        )
    variables: dict[str, Any] = {}
    vars_path = config_dir / VARS_FILE
    if vars_path.exists():
        variables = _validated_vars(vars_path, project)
    return overrides, variables


def _validated_csv(path: Path, columns: tuple[str, ...], check: Any) -> bytes:
    try:
        text = path.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError:
        raise ConfigError(f"{path.name}: not UTF-8 text") from None
    rows = list(csv.reader(io.StringIO(text)))
    if not rows or tuple(cell.strip() for cell in rows[0]) != columns:
        raise ConfigError(f"{path.name}: the first line must be {','.join(columns)}")
    data = [row for row in rows[1:] if any(cell.strip() for cell in row)]
    if not data:
        raise ConfigError(f"{path.name}: no rows")
    for number, row in enumerate(data, start=2):
        if len(row) != len(columns):
            raise ConfigError(f"{path.name} line {number}: expected {len(columns)} columns")
        try:
            check(dict(zip(columns, (cell.strip() for cell in row), strict=True)))
        except ValueError as exc:
            raise ConfigError(f"{path.name} line {number}: {exc}") from None
    # Normalize: strip whitespace, Unix line endings.
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(columns)
    writer.writerows([[cell.strip() for cell in row] for row in data])
    return out.getvalue().encode()


def _check_zone(row: dict[str, str]) -> None:
    if not row["zone"]:
        raise ValueError("zone is empty")
    try:
        network = ipaddress.ip_network(row["cidr"], strict=True)
    except ValueError as exc:
        raise ValueError(f"cidr {row['cidr']!r}: {exc}") from None
    if network.version != 4:
        raise ValueError(
            f"cidr {row['cidr']!r}: only IPv4 ranges are supported here; unique local and "
            "link-local IPv6 addresses are always internal"
        )
    if row["is_internal"].lower() not in ("true", "false"):
        raise ValueError("is_internal must be true or false")


def _check_allow(row: dict[str, str], rules: set[str]) -> None:
    if row["rule_id"] not in rules:
        raise ValueError(
            f"unknown rule_id {row['rule_id']!r}; use one of {', '.join(sorted(rules))}"
        )
    for column in ("src_ip", "dst_ip"):
        if row[column] != "*":
            try:
                ipaddress.ip_address(row[column])
            except ValueError:
                raise ValueError(f"{column} {row[column]!r} is not an IP address or *") from None
    port = row["dst_port"]
    if port != "*" and not (port.isdigit() and 0 <= int(port) <= 65_535):
        raise ValueError(f"dst_port {port!r} is not a port number or *")


def _known_rules(project: Path) -> set[str]:
    with (project / "seeds" / "detection_rules.csv").open(encoding="utf-8") as handle:
        return {row["rule_id"] for row in csv.DictReader(handle)}


def _validated_vars(path: Path, project: Path) -> dict[str, Any]:
    try:
        loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path.name}: not valid YAML: {exc}") from None
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise ConfigError(f"{path.name}: expected name: value pairs")
    defaults = yaml.safe_load((project / "dbt_project.yml").read_text(encoding="utf-8"))["vars"]
    for name, value in loaded.items():
        if name not in defaults:
            known = ", ".join(sorted(defaults))
            raise ConfigError(f"{path.name}: unknown setting {name!r}; known settings: {known}")
        default = defaults[name]
        number = isinstance(value, int | float) and not isinstance(value, bool)
        if not number or (isinstance(default, int) and not isinstance(value, int)):
            kind = "a whole number" if isinstance(default, int) else "a number"
            raise ConfigError(f"{path.name}: {name} must be {kind}")
        if value < 0:
            raise ConfigError(f"{path.name}: {name} must not be negative")
    return dict(loaded)
