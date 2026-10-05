"""End-to-end tests: ingestion, dbt build and the gold layer, on synthetic and real data."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pytest

from flowlake.bronze import Lake
from flowlake.evaluate import connect, evaluate, format_evaluation
from flowlake.ingest import discover, ingest_document, ingest_path, sensor_from_path
from flowlake.report import write_report
from flowlake.transform import build

pytestmark = pytest.mark.slow


def query(lake: Lake, sql: str) -> list[tuple[Any, ...]]:
    with connect(lake) as connection:
        return connection.execute(sql).fetchall()


def ingest_files(lake: Lake, files: list[Path], at: datetime) -> None:
    for file in files:
        sensor = sensor_from_path(file)
        assert sensor is not None
        result = ingest_document(
            lake,
            file.read_bytes(),
            sensor_id=sensor,
            source="flowsentinel_json",
            input_ref=str(file),
            ingested_at=at,
        )
        assert result.status == "ingested"


GOLD_SNAPSHOTS = {
    "fct_flows": "select * exclude (loaded_at) from gold.fct_flows order by record_id",
    "agg_traffic_hourly": (
        "select * exclude (max_loaded_at) from gold.agg_traffic_hourly order by all"
    ),
    "fct_alerts": "select * from gold.fct_alerts order by alert_id",
    "dim_hosts": "select * from gold.dim_hosts order by ip",
    "dim_host_names_scd2": "select * from gold.dim_host_names_scd2 order by host_name_key",
}


def snapshot(lake: Lake) -> dict[str, list[tuple[Any, ...]]]:
    return {name: query(lake, sql) for name, sql in GOLD_SNAPSHOTS.items()}


def test_detections_and_quarantine_match_the_ground_truth(
    built_lake: tuple[Lake, dict[str, Any]],
) -> None:
    lake, truth = built_lake
    evaluation = evaluate(lake, truth)
    assert evaluation.passed, format_evaluation(evaluation)
    assert evaluation.precision == evaluation.recall == 1.0


def test_gold_layer_is_consistent(built_lake: tuple[Lake, dict[str, Any]]) -> None:
    lake, truth = built_lake
    bronze = lake.bronze_flows
    distinct_bronze = duckdb.sql(
        f"select count(distinct record_id) from read_parquet('{bronze}/*/*.parquet')"
    ).fetchone()
    expected_flows = truth["flows_generated"] - sum(truth["expected_quarantine"].values())
    assert distinct_bronze == (expected_flows,)
    assert query(lake, "select count(*) from gold.fct_flows") == [(expected_flows,)]
    assert query(lake, "select count(*) from export.ocsf_network_activity") == [(expected_flows,)]
    assert query(
        lake,
        "select (select sum(bytes_total) from gold.fct_flows)"
        " = (select sum(bytes) from gold.agg_traffic_hourly)",
    ) == [(True,)]
    change = truth["hostname_reassignment"]
    versions = query(
        lake,
        f"select hostname, is_current from gold.dim_host_names_scd2 where ip = '{change['ip']}' "
        "order by version",
    )
    assert versions == [(change["names"][0], False), (change["names"][1], True)]


def test_ocsf_export_has_the_expected_shape(built_lake: tuple[Lake, dict[str, Any]]) -> None:
    lake, _ = built_lake
    row = query(
        lake,
        "select class_uid, type_uid, src_endpoint.ip, connection_info.direction_id,"
        " traffic.bytes = traffic.bytes_in + traffic.bytes_out, end_time >= start_time "
        "from export.ocsf_network_activity where connection_info.direction = 'Outbound' limit 1",
    )[0]
    assert row[0:2] == (4001, 400106)
    assert row[3] == 2 and row[4] is True and row[5] is True


def test_incremental_runs_with_late_data_match_a_full_refresh(
    tmp_path: Path, landing: tuple[Path, dict[str, Any]]
) -> None:
    """Three arrivals, one of them hours late, then a full refresh: the gold layer must be
    the same either way, and an incremental run must not rewrite rows outside its window."""
    lake = Lake(tmp_path / "lake")
    files = discover(landing[0])
    late = [f for f in files if "sensor-branch-20260928T0300Z" in f.name]
    assert len(late) == 1
    rest = [f for f in files if f not in late]
    now = datetime.now(UTC).replace(microsecond=0)

    # Half of the captures arrived in two rounds, three and two hours ago...
    ingest_files(lake, rest[: len(rest) // 3], now - timedelta(hours=3))
    ingest_files(lake, rest[len(rest) // 3 : len(rest) // 2], now - timedelta(hours=2))
    assert build(lake).success
    first_load = query(lake, "select max(loaded_at) from gold.fct_flows")[0][0]

    # ...the rest arrives now, including a capture from 03:00 on the first day (late data).
    ingest_files(lake, rest[len(rest) // 2 :] + late, now)
    assert build(lake).success
    late_hour = datetime(2026, 9, 28, 3, tzinfo=UTC)
    assert query(
        lake,
        f"select count(*) > 0 from gold.fct_flows where capture_file like "
        f"'sensor-branch-20260928T0300Z%' and flow_hour = '{late_hour.isoformat()}'",
    ) == [(True,)]
    # Rows ingested three hours ago are outside the 30-minute lookback: not rewritten.
    untouched = query(
        lake,
        "select count(*), max(loaded_at) from gold.fct_flows "
        f"where ingested_at = '{(now - timedelta(hours=3)).isoformat()}'",
    )[0]
    assert untouched[0] > 0 and untouched[1] == first_load
    incremental = snapshot(lake)

    assert build(lake, full_refresh=True).success
    assert snapshot(lake) == incremental


def test_replayed_batches_do_not_duplicate_flows(
    tmp_path: Path, landing: tuple[Path, dict[str, Any]]
) -> None:
    lake = Lake(tmp_path / "lake")
    files = discover(landing[0])[:6]
    ingest_files(lake, files, datetime.now(UTC) - timedelta(days=2))
    assert build(lake).success
    flows = query(lake, "select count(*) from gold.fct_flows")[0][0]

    # The same captures again on another day: new bronze files, the same record IDs.
    for file in files:
        sensor = sensor_from_path(file)
        assert sensor is not None
        ingest_document(
            lake,
            file.read_bytes(),
            sensor_id=sensor,
            source="flowsentinel_json",
            input_ref=str(file),
            force=True,
        )
    bronze_rows = duckdb.sql(
        f"select count(*) from read_parquet('{lake.bronze_flows}/*/*.parquet')"
    ).fetchone()
    assert bronze_rows == (2 * flows,)
    assert build(lake).success
    assert query(lake, "select count(*), count(distinct record_id) from gold.fct_flows") == [
        (flows, flows)
    ]


def test_an_empty_lake_builds_and_real_flowsentinel_data_flows_through(
    tmp_path: Path, fixtures_dir: Path
) -> None:
    lake = Lake(tmp_path / "lake")
    assert build(lake).success  # nothing ingested yet
    assert query(lake, "select count(*) from gold.fct_flows") == [(0,)]

    ingest_path(lake, fixtures_dir, sensor_id="lab")
    assert build(lake).success  # incremental, from an empty target
    assert query(lake, "select count(*) from gold.fct_flows") == [(85,)]
    directions = dict(query(lake, "select ip_version, count(*) from gold.fct_flows group by 1"))
    assert directions[6] > 0 and directions[4] > 0
    assert query(
        lake,
        "select responder_name from gold.fct_flows where http_hosts[1] = 'www.example.com:8080'",
    ) == [("www.example.com",)]


def test_report_renders_every_alert(
    built_lake: tuple[Lake, dict[str, Any]], tmp_path: Path
) -> None:
    lake, truth = built_lake
    page = write_report(lake, tmp_path / "site", ground_truth=truth).read_text()
    for name in ("Large outbound transfer", "Periodic beaconing", "DNS tunneling", "Port scan"):
        assert name in page
    assert "Synthetic demo data" in page
    assert page.count('<tr><td><span class="sev') == 4
