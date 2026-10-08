"""Dagster definitions: the lakehouse as software-defined assets.

    landing files ──▶ bronze/flows + bronze/quarantine ──▶ dbt models and tests ──▶ dashboard
                       (Python, contract-checked)          (one asset per model,     (static
                                                            tests as asset checks)    HTML)

Run ``dagster dev`` in the repository root (``pyproject.toml`` points Dagster at this module).
Paths come from FLOWLAKE_LAKE, FLOWLAKE_LANDING and FLOWLAKE_SITE (defaults: ./lake,
./landing, ./site). A sensor starts a run when new files land; a schedule runs hourly anyway,
so source freshness and late data are handled even when nothing new arrives.
"""

# No `from __future__ import annotations` here: Dagster inspects the runtime type hints.
import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import duckdb
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    AssetSpec,
    ConfigurableResource,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    EnvVar,
    MaterializeResult,
    MetadataValue,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    SkipReason,
    asset,
    asset_check,
    define_asset_job,
    multi_asset,
    sensor,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets, get_asset_key_for_model

from flowlake.bronze import Lake
from flowlake.ingest import discover, ingest_path
from flowlake.report import write_report
from flowlake.transform import dbt_project_dir

BRONZE_FLOWS = AssetKey(["bronze", "flows"])
BRONZE_QUARANTINE = AssetKey(["bronze", "quarantine"])
MAX_QUARANTINE_RATE = 0.05


class LakehouseResource(ConfigurableResource):  # type: ignore[type-arg]
    """Where the lake, the landing directory and the dashboard live."""

    lake_root: str
    landing_dir: str
    site_dir: str

    @property
    def lake(self) -> Lake:
        return Lake(Path(self.lake_root).resolve())

    def export_lake_path(self) -> None:
        """The dbt profile reads the lake root from FLOWLAKE_LAKE."""
        os.environ["FLOWLAKE_LAKE"] = str(Path(self.lake_root).resolve())


def _dbt_executable() -> str:
    """The dbt that belongs to this interpreter, even when its bin directory is not on PATH."""
    found = shutil.which("dbt", path=str(Path(sys.executable).parent)) or shutil.which("dbt")
    return found or "dbt"


def _prepare(project: DbtProject) -> DbtProject:
    """Make sure a manifest exists: parsed on every load under ``dagster dev`` (which sets
    DAGSTER_IS_DEV_CLI), once otherwise.

    This replaces ``DbtProject.prepare_if_dev()``, which looks for ``dbt`` on PATH and so fails
    when the virtual environment is not activated (``.venv/bin/dagster dev``). Several Dagster
    processes load this module at the same time, so each parses into its own directory and
    swaps the manifest into place atomically: nobody reads a half-written file.
    """
    if os.environ.get("DAGSTER_IS_DEV_CLI") or not project.manifest_path.exists():
        project.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=project.manifest_path.parent) as scratch:
            subprocess.run(
                [
                    _dbt_executable(),
                    "parse",
                    "--quiet",
                    "--project-dir",
                    str(project.project_dir),
                    "--profiles-dir",
                    str(project.profiles_dir),
                    "--target-path",
                    scratch,
                    "--log-path",
                    scratch,
                ],
                check=True,
            )
            (Path(scratch) / "manifest.json").replace(project.manifest_path)
    return project


dbt_project = _prepare(DbtProject(project_dir=dbt_project_dir(), profiles_dir=dbt_project_dir()))


def dbt_resource() -> DbtCliResource:
    return DbtCliResource(
        project_dir=dbt_project,
        profiles_dir=dbt_project.profiles_dir,
        dbt_executable=_dbt_executable(),
    )


@multi_asset(
    specs=[
        AssetSpec(
            BRONZE_FLOWS,
            description="Contract-checked flow records (Parquet).",
            group_name="bronze",
            kinds={"python", "parquet"},
        ),
        AssetSpec(
            BRONZE_QUARANTINE,
            description="Records that failed the contract.",
            group_name="bronze",
            kinds={"python", "parquet"},
        ),
    ],
    can_subset=False,
)
def bronze(
    context: AssetExecutionContext, lakehouse: LakehouseResource
) -> Iterator[MaterializeResult]:  # type: ignore[type-arg]
    """Ingest every new file in the landing directory. Finished batches are skipped."""
    lake = lakehouse.lake.ensure()
    landing = Path(lakehouse.landing_dir)
    results = ingest_path(lake, landing, workers=os.cpu_count() or 1) if landing.exists() else []
    ingested = [r for r in results if r.status == "ingested"]
    failed = [r for r in results if r.status == "failed"]
    for result in failed:
        context.log.warning(f"failed, will retry: {result.input_ref}: {result.error}")
    written = sum(r.records_written for r in ingested)
    quarantined = sum(r.records_quarantined for r in ingested)
    yield MaterializeResult(
        asset_key=BRONZE_FLOWS,
        metadata={
            "batches_ingested": len(ingested),
            "batches_skipped": sum(r.status == "skipped" for r in results),
            "batches_rejected": sum(r.status == "rejected" for r in results),
            "batches_failed": len(failed),
            "records_written": written,
            "lake": MetadataValue.path(str(lake.root)),
        },
    )
    yield MaterializeResult(
        asset_key=BRONZE_QUARANTINE,
        metadata={"records_quarantined": quarantined},
    )


@asset_check(asset=BRONZE_FLOWS, description="At most 5% of all records were quarantined.")
def quarantine_rate_is_low(lakehouse: LakehouseResource) -> AssetCheckResult:
    lake = lakehouse.lake.ensure()
    accepted, rejected = duckdb.execute(
        "select (select count(*) from read_parquet($flows)),"
        " (select count(*) from read_parquet($quarantine))",
        {
            "flows": f"{lake.bronze_flows}/*/*.parquet",
            "quarantine": f"{lake.quarantine}/*/*.parquet",
        },
    ).fetchone() or (0, 0)
    total = accepted + rejected
    rate = rejected / total if total else 0.0
    return AssetCheckResult(
        passed=rate <= MAX_QUARANTINE_RATE,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "quarantine_rate": rate,
            "records_accepted": accepted,
            "records_quarantined": rejected,
        },
    )


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project)
def dbt_models(
    context: AssetExecutionContext, dbt: DbtCliResource, lakehouse: LakehouseResource
) -> Iterator[Any]:
    """Every dbt model and seed is an asset; every dbt test is an asset check."""
    lakehouse.export_lake_path()
    yield from dbt.cli(["build"], context=context).stream()


@asset(
    deps=[
        get_asset_key_for_model([dbt_models], model)
        for model in (
            "fct_alerts",
            "agg_traffic_hourly",
            "dim_hosts",
            "dq_batches",
            "dim_host_names_scd2",
        )
    ],
    group_name="serving",
    kinds={"html"},
    description="The static HTML dashboard.",
)
def dashboard(lakehouse: LakehouseResource) -> MaterializeResult:  # type: ignore[type-arg]
    path = write_report(lakehouse.lake, Path(lakehouse.site_dir))
    return MaterializeResult(metadata={"path": MetadataValue.path(str(path.resolve()))})


lakehouse_job = define_asset_job("lakehouse_pipeline", selection=AssetSelection.all())

hourly = ScheduleDefinition(
    job=lakehouse_job, cron_schedule="7 * * * *", default_status=DefaultScheduleStatus.STOPPED
)


@sensor(job=lakehouse_job, minimum_interval_seconds=60, default_status=DefaultSensorStatus.STOPPED)
def new_landing_files(context: SensorEvaluationContext) -> RunRequest | SkipReason:
    """Start a run when files newer than the last seen one land."""
    landing = Path(EnvVar("FLOWLAKE_LANDING").get_value("landing") or "landing")
    files = discover(landing) if landing.exists() else []
    newest = max((f.stat().st_mtime for f in files), default=0.0)
    if newest <= float(context.cursor or 0):
        return SkipReason("no new files in the landing directory")
    context.update_cursor(str(newest))
    return RunRequest(run_key=f"landing-{newest}")


defs = Definitions(
    assets=[bronze, dbt_models, dashboard],
    asset_checks=[quarantine_rate_is_low],
    jobs=[lakehouse_job],
    schedules=[hourly],
    sensors=[new_landing_files],
    resources={
        "dbt": dbt_resource(),
        "lakehouse": LakehouseResource(
            lake_root=EnvVar("FLOWLAKE_LAKE").get_value("lake") or "lake",
            landing_dir=EnvVar("FLOWLAKE_LANDING").get_value("landing") or "landing",
            site_dir=EnvVar("FLOWLAKE_SITE").get_value("site") or "site",
        ),
    },
)
