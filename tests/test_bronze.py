from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from flowlake.bronze import (
    BRONZE_FLOW_SCHEMA,
    QUARANTINE_SCHEMA,
    BatchWriter,
    Lake,
    RecordContext,
    record_id,
)

from .conftest import load_fixture

CONTEXT = RecordContext("lab", "sha256:abc", "capture.pcap", "complete")
WHEN = datetime(2026, 10, 1, 12, 30, tzinfo=UTC)


def test_record_id_is_deterministic_and_distinguishes_its_parts() -> None:
    assert record_id("a", "c", 1) == record_id("a", "c", 1)
    assert (
        len(
            {
                record_id("a", "c", 1),
                record_id("b", "c", 1),
                record_id("a", "d", 1),
                record_id("a", "c", 2),
            }
        )
        == 4
    )
    assert len(record_id("a", "c", 1)) == 32


def test_ensure_creates_readable_empty_datasets(lake: Lake) -> None:
    for dataset in (lake.bronze_flows, lake.quarantine):
        count = duckdb.sql(
            f"select count(*) from read_parquet('{dataset}/*/*.parquet', hive_partitioning=true)"
        ).fetchone()
        assert count == (0,)
    lake.ensure()  # idempotent


def test_commit_writes_partitioned_files_and_a_ledger_entry(lake: Lake) -> None:
    flows = load_fixture("flows-mixed")["flows"]
    writer = BatchWriter(lake, "batch-1", source="test", input_ref="memory", ingested_at=WHEN)
    for flow in flows:
        assert writer.add(flow, CONTEXT)
    writer.add({"not": "a flow"}, CONTEXT)
    result = writer.commit(extra="value")

    assert (result.records_in, result.records_written, result.records_quarantined) == (7, 6, 1)
    assert result.quarantine_reasons == {"missing": 1}
    flows_file = lake.bronze_flows / "ingest_date=2026-10-01" / "batch-1.parquet"
    quarantine_file = lake.quarantine / "ingest_date=2026-10-01" / "batch-1.parquet"
    assert pq.read_schema(flows_file).equals(BRONZE_FLOW_SCHEMA)
    assert pq.read_schema(quarantine_file).equals(QUARANTINE_SCHEMA)
    table = pq.read_table(flows_file)
    assert table.num_rows == 6
    assert set(table.column("ingested_at").to_pylist()) == {WHEN}
    assert table.column("record_id").to_pylist()[0] == record_id("lab", "sha256:abc", 1)

    entry = lake.ledger_entry("batch-1")
    assert entry is not None
    assert entry["status"] == "ingested" and entry["extra"] == "value"
    assert not list(Path(lake.root).rglob("*.tmp")), "temporary files must not remain"


def test_quarantine_keeps_the_raw_record_and_the_reason(lake: Lake) -> None:
    flow = load_fixture("flows-mixed")["flows"][0] | {"bytes_total": 1}
    writer = BatchWriter(lake, "batch-q", source="test", input_ref="memory", ingested_at=WHEN)
    assert not writer.add(flow, CONTEXT)
    writer.commit()
    row = pq.read_table(lake.quarantine / "ingest_date=2026-10-01" / "batch-q.parquet").to_pylist()[
        0
    ]
    assert row["error_type"] == "totals_mismatch"
    assert '"bytes_total": 1' in row["raw_record"]
    assert row["capture_id"] == "sha256:abc"
    assert not (lake.bronze_flows / "ingest_date=2026-10-01" / "batch-q.parquet").exists()


def test_ingested_at_defaults_to_commit_time(lake: Lake) -> None:
    writer = BatchWriter(lake, "batch-now", source="test", input_ref="memory")
    writer.add(load_fixture("flows-mixed")["flows"][0], CONTEXT)
    before = datetime.now(UTC)
    result = writer.commit()
    assert result.ingested_at is not None
    assert datetime.fromisoformat(result.ingested_at) >= before


@pytest.mark.parametrize("batch_id", ["", "../escape", "a/b", "c:d", ".hidden"])
def test_unsafe_batch_ids_are_rejected(lake: Lake, batch_id: str) -> None:
    with pytest.raises(ValueError, match="unsafe batch id"):
        BatchWriter(lake, batch_id, source="test", input_ref="memory")


def test_naive_timestamps_are_rejected(lake: Lake) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        BatchWriter(lake, "b", source="test", input_ref="memory", ingested_at=datetime(2026, 1, 1))
