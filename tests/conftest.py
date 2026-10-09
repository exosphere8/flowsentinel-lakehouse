from __future__ import annotations

import json
import os
import shutil
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from flowlake.bronze import Lake
from flowlake.ingest import ingest_path
from flowlake.sources.synthetic import SyntheticConfig, generate
from flowlake.transform import build

FIXTURES = Path(__file__).parent / "fixtures" / "flowsentinel"

# Modules that read FLOWLAKE_LAKE at import (the Dagster definitions) must not touch ./lake.
os.environ.setdefault("FLOWLAKE_LAKE", tempfile.mkdtemp(prefix="flowlake-tests-"))

# Small enough to be quick, large enough that every incident and corruption type appears.
SMALL = SyntheticConfig(days=3, workstations_per_sensor=10, corrupt_rate=0.004)


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES


@pytest.fixture
def lake(tmp_path: Path) -> Lake:
    return Lake(tmp_path / "lake").ensure()


def load_fixture(name: str) -> dict[str, Any]:
    document: dict[str, Any] = json.loads((FIXTURES / f"{name}.json").read_text())
    return document


def all_fixture_flows() -> list[dict[str, Any]]:
    flows = []
    for path in sorted(FIXTURES.glob("*.json")):
        document = json.loads(path.read_text())
        flows.extend(document.get("flows", []))
    return flows


@pytest.fixture(scope="session")
def landing(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, dict[str, Any]]:
    """Synthetic landing directory shared by the pipeline tests (read-only)."""
    out = tmp_path_factory.mktemp("landing")
    truth = generate(SMALL, out)
    return out, truth


@pytest.fixture(scope="session")
def built_lake(
    tmp_path_factory: pytest.TempPathFactory, landing: tuple[Path, dict[str, Any]]
) -> Iterator[tuple[Lake, dict[str, Any]]]:
    """A lake with the synthetic data ingested and dbt built once (read-only for tests)."""
    root = tmp_path_factory.mktemp("built") / "lake"
    lake = Lake(root)
    ingest_path(lake, landing[0], workers=2)
    assert build(lake).success
    yield lake, landing[1]
    shutil.rmtree(root, ignore_errors=True)
