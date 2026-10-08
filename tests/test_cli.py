from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any

import duckdb
import pytest

from flowlake.bronze import Lake
from flowlake.cli import main
from flowlake.evaluate import connect


def test_contract_check_passes_on_the_committed_schemas() -> None:
    assert main(["contract", "--check", "--out", str(Path(__file__).parents[1] / "contracts")]) == 0


def test_contract_check_fails_on_stale_schemas(tmp_path: Path) -> None:
    assert main(["contract", "--out", str(tmp_path)]) == 0
    (tmp_path / "flow_record.v1.schema.json").write_text("{}")
    assert main(["contract", "--check", "--out", str(tmp_path)]) == 1


def test_generate_and_ingest(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    landing = tmp_path / "landing"
    assert main(["generate", "--out", str(landing), "--days", "1", "--workstations", "2"]) == 0
    truth = json.loads((landing / "_ground_truth.json").read_text())
    assert truth["captures"] == 48
    assert main(["--lake", str(tmp_path / "lake"), "ingest", str(landing)]) == 0
    output = capsys.readouterr().out
    assert "batches: {'ingested': 48}" in output
    assert main(["--lake", str(tmp_path / "lake"), "ingest", str(landing)]) == 0
    assert "batches: {'skipped': 48}" in capsys.readouterr().out


def test_ingest_reports_rejected_inputs(
    tmp_path: Path, fixtures_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(
        [
            "--lake",
            str(tmp_path / "lake"),
            "ingest",
            str(fixtures_dir / "invalid-magic.json"),
            "--sensor",
            "lab",
        ]
    )
    assert code == 0  # a rejection is recorded, not an error
    assert "rejected" in capsys.readouterr().out


def test_usage_errors_exit_with_2(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main([]) == 2
    assert main(["--lake", str(tmp_path), "ingest", str(tmp_path / "missing")]) == 2
    assert "no such file or directory" in capsys.readouterr().err
    assert (
        main(["--lake", str(tmp_path / "lake"), "ingest", str(tmp_path), "--sensor", "bad sensor!"])
        == 2
    )


@pytest.mark.slow
def test_evaluate_and_report_commands(
    built_lake: tuple[Lake, dict[str, Any]],
    landing: tuple[Path, dict[str, Any]],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    lake, _ = built_lake
    truth = landing[0] / "_ground_truth.json"
    assert main(["--lake", str(lake.root), "evaluate", str(truth)]) == 0
    assert "quarantine matches" in capsys.readouterr().out
    site = tmp_path / "site"
    assert (
        main(["--lake", str(lake.root), "report", "--out", str(site), "--ground-truth", str(truth)])
        == 0
    )
    assert "Scored against the synthetic ground truth" in (site / "index.html").read_text()


@pytest.mark.slow
def test_transform_vars_tune_the_detections(tmp_path: Path, fixtures_dir: Path) -> None:
    lake = Lake(tmp_path / "lake")
    assert main(["--lake", str(lake.root), "ingest", str(fixtures_dir), "--sensor", "lab"]) == 0

    def port_scans() -> int:
        with connect(lake) as connection:
            row = connection.execute("select count(*) from gold.det_port_scan").fetchone()
        return int(row[0]) if row else 0

    assert main(["--lake", str(lake.root), "transform", "--freshness"]) == 0
    default = port_scans()
    assert (
        main(
            [
                "--lake",
                str(lake.root),
                "transform",
                "--vars",
                '{"port_scan_min_distinct_ports": 3, "port_scan_min_failed_ratio": 0.1}',
            ]
        )
        == 0
    )
    assert port_scans() > default


@pytest.mark.kafka
@pytest.mark.skipif(
    not os.environ.get("FLOWLAKE_KAFKA_BOOTSTRAP"), reason="FLOWLAKE_KAFKA_BOOTSTRAP is not set"
)
def test_stream_commands_round_trip(tmp_path: Path, fixtures_dir: Path) -> None:
    topic = f"test.cli.{uuid.uuid4().hex[:8]}"
    assert main(["stream", "produce", str(fixtures_dir), "--sensor", "lab", "--topic", topic]) == 0
    lake = Lake(tmp_path / "lake")
    assert (
        main(
            [
                "--lake",
                str(lake.root),
                "stream",
                "consume",
                "--topic",
                topic,
                "--group",
                "cli-test",
                "--idle-timeout",
                "5",
            ]
        )
        == 0
    )
    rows = duckdb.sql(
        f"select count(distinct record_id) from read_parquet('{lake.bronze_flows}/*/*.parquet')"
    ).fetchone()
    assert rows == (85,)
