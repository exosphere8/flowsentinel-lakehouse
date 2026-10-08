"""The Dagster definitions load and materialize the whole lakehouse in-process."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

dagster = pytest.importorskip("dagster")
pytest.importorskip("dagster_dbt")

from dagster import (
    AssetKey,
    DagsterInstance,
    RunRequest,
    SkipReason,
    build_sensor_context,
    materialize,
)

from flowlake.bronze import Lake
from flowlake.evaluate import connect

pytestmark = pytest.mark.slow


def test_definitions_load_with_every_layer() -> None:
    from flowlake.orchestration import defs

    keys = {key.to_user_string() for key in defs.resolve_asset_graph().get_all_asset_keys()}
    assert {
        "bronze/flows",
        "bronze/quarantine",
        "gold/fct_flows",
        "gold/fct_alerts",
        "export/ocsf_network_activity",
        "dashboard",
    } <= keys
    assert defs.resolve_job_def("lakehouse_pipeline") is not None


def test_the_pipeline_materializes_end_to_end(
    tmp_path: Path, landing: tuple[Path, dict[str, Any]]
) -> None:
    from flowlake.orchestration import (
        LakehouseResource,
        bronze,
        dashboard,
        dbt_models,
        dbt_resource,
        quarantine_rate_is_low,
    )

    lakehouse = LakehouseResource(
        lake_root=str(tmp_path / "lake"),
        landing_dir=str(landing[0]),
        site_dir=str(tmp_path / "site"),
    )
    result = materialize(
        [bronze, dbt_models, dashboard],
        resources={"lakehouse": lakehouse, "dbt": dbt_resource()},
        instance=DagsterInstance.ephemeral(),
    )
    assert result.success
    materialized = {event.asset_key for event in result.get_asset_materialization_events()}
    assert {
        AssetKey(["bronze", "flows"]),
        AssetKey(["gold", "fct_alerts"]),
        AssetKey("dashboard"),
    } <= materialized
    checks = result.get_asset_check_evaluations()
    assert checks and all(check.passed for check in checks if check.severity.value == "ERROR")
    assert (tmp_path / "site" / "index.html").exists()
    with connect(Lake(tmp_path / "lake")) as connection:
        assert connection.execute("select count(*) from gold.fct_alerts").fetchone() == (4,)

    check = quarantine_rate_is_low(lakehouse)
    assert check.passed


def test_the_sensor_fires_once_per_new_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from flowlake.orchestration import new_landing_files

    monkeypatch.setenv("FLOWLAKE_LANDING", str(tmp_path))
    context = build_sensor_context()
    assert isinstance(new_landing_files(context), SkipReason)
    (tmp_path / "sensor=a").mkdir()
    (tmp_path / "sensor=a" / "capture.json").write_text("{}")
    assert isinstance(new_landing_files(context), RunRequest)
    assert isinstance(new_landing_files(context), SkipReason)
