"""Run the dbt project against a lake."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from flowlake.bronze import Lake


def dbt_project_dir() -> Path:
    """The dbt project: FLOWLAKE_DBT_PROJECT, else ``transform/`` in this checkout."""
    configured = os.environ.get("FLOWLAKE_DBT_PROJECT")
    project = Path(configured) if configured else Path(__file__).resolve().parents[2] / "transform"
    if not (project / "dbt_project.yml").is_file():
        raise FileNotFoundError(
            f"dbt project not found at {project}: run from a checkout of the repository or set "
            "FLOWLAKE_DBT_PROJECT"
        )
    return project


@dataclass(frozen=True)
class DbtResult:
    success: bool
    command: list[str]


def dbt_args(lake: Lake, command: Sequence[str]) -> list[str]:
    """dbt arguments for one lake. Artifacts go under the lake, so lakes never share state."""
    project = dbt_project_dir()
    state = lake.root.resolve() / ".dbt"
    return [
        *command,
        "--project-dir",
        str(project),
        "--profiles-dir",
        str(project),
        "--target-path",
        str(state / "target"),
        "--log-path",
        str(state / "logs"),
    ]


def run_dbt(lake: Lake, command: Sequence[str]) -> DbtResult:
    """Run dbt in a child process with FLOWLAKE_LAKE pointing at ``lake``.

    A separate process, like an orchestrator would use: dbt's DuckDB connection is closed when
    it exits, so this process can open the warehouse read-only afterwards.
    """
    lake.ensure()
    args = dbt_args(lake, command)
    environment = os.environ | {"FLOWLAKE_LAKE": str(lake.root.resolve())}
    # The same entry point as the `dbt` console script, run by this interpreter.
    completed = subprocess.run(
        [sys.executable, "-c", "from dbt.cli.main import cli; cli()", *args],
        env=environment,
        check=False,
    )
    return DbtResult(success=completed.returncode == 0, command=args)


def build(
    lake: Lake,
    *,
    full_refresh: bool = False,
    select: str | None = None,
    variables: Mapping[str, Any] | None = None,
) -> DbtResult:
    """``dbt build``. ``variables`` override dbt vars, for example detection thresholds."""
    command = ["build"]
    if full_refresh:
        command.append("--full-refresh")
    if select:
        command += ["--select", select]
    if variables:
        command += ["--vars", json.dumps(dict(variables))]
    return run_dbt(lake, command)


def source_freshness(lake: Lake) -> DbtResult:
    return run_dbt(lake, ["source", "freshness"])
