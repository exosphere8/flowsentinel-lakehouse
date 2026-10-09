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
from flowlake.project import ProjectSettings, config_dir_from_env, prepare_project


@dataclass(frozen=True)
class DbtResult:
    success: bool
    command: list[str]


def dbt_args(settings: ProjectSettings, lake: Lake, command: Sequence[str]) -> list[str]:
    """dbt arguments for one lake. Artifacts go under the lake, so lakes never share state."""
    state = lake.root.resolve() / ".dbt"
    return [
        *command,
        "--project-dir",
        str(settings.project_dir),
        "--profiles-dir",
        str(settings.project_dir),
        "--target-path",
        str(state / "target"),
        "--log-path",
        str(state / "logs"),
    ]


def run_dbt(
    lake: Lake,
    command: Sequence[str],
    *,
    config_dir: Path | None = None,
    variables: Mapping[str, Any] | None = None,
) -> DbtResult:
    """Run dbt in a child process with FLOWLAKE_LAKE pointing at ``lake``.

    A separate process, like an orchestrator would use: dbt's DuckDB connection is closed when
    it exits, so this process can open the warehouse read-only afterwards. ``config_dir``
    defaults to FLOWLAKE_CONFIG; ``variables`` override the configured dbt vars.
    """
    lake.ensure()
    settings = prepare_project(lake, config_dir or config_dir_from_env())
    merged = {**settings.variables, **(variables or {})}
    args = dbt_args(settings, lake, command)
    if merged:
        args += ["--vars", json.dumps(merged)]
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
    config_dir: Path | None = None,
) -> DbtResult:
    """``dbt build``. ``variables`` override dbt vars, for example detection thresholds."""
    command = ["build"]
    if full_refresh:
        command.append("--full-refresh")
    if select:
        command += ["--select", select]
    return run_dbt(lake, command, config_dir=config_dir, variables=variables)


def source_freshness(lake: Lake, *, config_dir: Path | None = None) -> DbtResult:
    return run_dbt(lake, ["source", "freshness"], config_dir=config_dir)
