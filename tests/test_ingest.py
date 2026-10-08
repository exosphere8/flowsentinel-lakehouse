from __future__ import annotations

import stat
import sys
from pathlib import Path

import duckdb
import pytest

from flowlake.bronze import Lake
from flowlake.ingest import discover, ingest_path, ingest_pcap, sensor_from_path


def bronze_record_ids(lake: Lake) -> list[str]:
    rows = duckdb.sql(
        f"select record_id from read_parquet('{lake.bronze_flows}/*/*.parquet') order by 1"
    ).fetchall()
    return [row[0] for row in rows]


def test_ingests_real_flowsentinel_documents(lake: Lake, fixtures_dir: Path) -> None:
    results = {Path(r.input_ref).stem: r for r in ingest_path(lake, fixtures_dir, sensor_id="lab")}
    assert results["detect-mixed"].records_written == 47
    assert results["flows-mixed"].status == "ingested"
    rejected = results["invalid-magic"]
    assert rejected.status == "rejected"
    assert rejected.error is not None and rejected.error.startswith("invalid_magic (malformed)")
    assert sum(r.records_written for r in results.values()) == 85
    assert len(set(bronze_record_ids(lake))) == 85


def test_finished_batches_are_skipped_and_force_rewrites_the_same_files(
    lake: Lake, fixtures_dir: Path
) -> None:
    first = ingest_path(lake, fixtures_dir, sensor_id="lab")
    assert {r.status for r in ingest_path(lake, fixtures_dir, sensor_id="lab")} == {"skipped"}
    files_before = sorted(p.name for p in lake.bronze_flows.rglob("*.parquet"))
    again = ingest_path(lake, fixtures_dir, sensor_id="lab", force=True)
    assert [r.batch_id for r in again] == [r.batch_id for r in first]
    # Same day, same batch IDs: the same files are overwritten, nothing is duplicated.
    assert sorted(p.name for p in lake.bronze_flows.rglob("*.parquet")) == files_before
    assert len(bronze_record_ids(lake)) == 85


def test_parallel_ingestion_matches_serial(tmp_path: Path, fixtures_dir: Path) -> None:
    serial, parallel = Lake(tmp_path / "serial"), Lake(tmp_path / "parallel")
    ingest_path(serial, fixtures_dir, sensor_id="lab")
    ingest_path(parallel, fixtures_dir, sensor_id="lab", workers=3)
    assert bronze_record_ids(serial) == bronze_record_ids(parallel)


def test_the_same_capture_from_two_sensors_is_two_batches(lake: Lake, fixtures_dir: Path) -> None:
    path = fixtures_dir / "app-tls.json"
    a = ingest_path(lake, path, sensor_id="sensor-a")[0]
    b = ingest_path(lake, path, sensor_id="sensor-b")[0]
    assert a.batch_id != b.batch_id and b.status == "ingested"


def test_discovery_and_sensor_ids_come_from_the_path(tmp_path: Path) -> None:
    for name in [
        "sensor=edge-1/a.json",
        "sensor=edge-1/b.PCAP",
        "sensor=edge-1/notes.txt",
        "_ground_truth.json",
        ".hidden/c.json",
        "sensor=edge-2/_tmp/d.json",
    ]:
        (tmp_path / name).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / name).write_text("{}")
    found = [p.relative_to(tmp_path).as_posix() for p in discover(tmp_path)]
    assert found == ["sensor=edge-1/a.json", "sensor=edge-1/b.PCAP"]
    assert sensor_from_path(tmp_path / "sensor=edge-1" / "a.json") == "edge-1"
    assert sensor_from_path(tmp_path / "a.json") is None
    with pytest.raises(ValueError, match="invalid sensor id"):
        sensor_from_path(tmp_path / "sensor=bad id!" / "a.json")


def test_files_that_are_not_flow_documents_are_rejected(lake: Lake, tmp_path: Path) -> None:
    (tmp_path / "broken.json").write_text("{not json")
    (tmp_path / "other.json").write_text('{"hello": "world"}')
    results = ingest_path(lake, tmp_path, sensor_id="lab")
    assert [r.status for r in results] == ["rejected", "rejected"]
    assert "not valid JSON" in (results[0].error or "")
    assert "not a FlowSentinel flows document" in (results[1].error or "")


def fake_flowsentinel(tmp_path: Path, document: Path, exit_code: int = 0) -> Path:
    """A stand-in for the flowsentinel binary that prints a fixture document."""
    script = tmp_path / "flowsentinel"
    script.write_text(
        f"#!{sys.executable}\n"
        "import sys\n"
        "assert sys.argv[1:4] == ['flows', '--json', '--pcap'], sys.argv\n"
        f"sys.stdout.write(open({str(document)!r}).read())\n"
        f"sys.exit({exit_code})\n"
    )
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    return script


def test_pcaps_are_run_through_the_flowsentinel_cli(
    lake: Lake, tmp_path: Path, fixtures_dir: Path
) -> None:
    pcap = tmp_path / "capture.pcap"
    pcap.write_bytes(b"pretend pcap bytes")
    binary = fake_flowsentinel(tmp_path, fixtures_dir / "app-http.json")
    result = ingest_pcap(lake, pcap, sensor_id="lab", binary=str(binary))
    assert (result.status, result.source, result.records_written) == (
        "ingested",
        "flowsentinel_pcap",
        5,
    )
    # The capture is identified by the pcap's hash, so a second run is skipped without the CLI.
    assert ingest_pcap(lake, pcap, sensor_id="lab", binary="/nonexistent").status == "skipped"


def test_a_pcap_rejected_by_flowsentinel_is_recorded(
    lake: Lake, tmp_path: Path, fixtures_dir: Path
) -> None:
    pcap = tmp_path / "bad.pcap"
    pcap.write_bytes(b"garbage")
    binary = fake_flowsentinel(tmp_path, fixtures_dir / "invalid-magic.json", exit_code=4)
    result = ingest_pcap(lake, pcap, sensor_id="lab", binary=str(binary))
    assert result.status == "rejected" and "invalid_magic" in (result.error or "")
    assert lake.ledger_entry(result.batch_id) is not None


def test_a_missing_binary_fails_without_a_ledger_entry_so_it_is_retried(
    lake: Lake, tmp_path: Path, fixtures_dir: Path
) -> None:
    pcap = tmp_path / "x.pcap"
    pcap.write_bytes(b"x")
    result = ingest_pcap(lake, pcap, sensor_id="lab", binary=str(tmp_path / "missing"))
    assert result.status == "failed" and "could not run flowsentinel" in (result.error or "")
    assert lake.ledger_entry(result.batch_id) is None
    binary = fake_flowsentinel(tmp_path, fixtures_dir / "app-tls.json")
    assert ingest_pcap(lake, pcap, sensor_id="lab", binary=str(binary)).status == "ingested"
