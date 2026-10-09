"""The streaming path. Unit tests run anywhere; the ``kafka`` tests need a broker at
FLOWLAKE_KAFKA_BOOTSTRAP (``docker compose up -d --wait`` provides one on localhost:19092)."""

from __future__ import annotations

import json
import os
import uuid
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pytest

from flowlake.bronze import BatchWriter, Lake
from flowlake.ingest import ingest_path
from flowlake.sources.flowsentinel import sha256_hex
from flowlake.sources.synthetic import SyntheticConfig, SyntheticNetwork, generate, serialize
from flowlake.streaming import CaptureFlows, add_message, kafka_batch_id, messages

from .conftest import load_fixture

BOOTSTRAP = os.environ.get("FLOWLAKE_KAFKA_BOOTSTRAP") or None  # empty means unset
CONFIG = SyntheticConfig(
    start=date(2026, 9, 28), days=1, workstations_per_sensor=4, corrupt_rate=0.01
)


def capture_flows(config: SyntheticConfig) -> list[CaptureFlows]:
    result = []
    for capture in SyntheticNetwork(config).captures():
        document = capture.document
        result.append(
            CaptureFlows(
                capture.sensor_id,
                f"sha256:{sha256_hex(serialize(document))}",
                document["capture"]["file_name"],
                document["completion_state"],
                document["flows"],
            )
        )
    return result


def record_ids(lake: Lake) -> list[str]:
    rows = duckdb.sql(
        f"select record_id from read_parquet('{lake.bronze_flows}/*/*.parquet') order by 1"
    ).fetchall()
    return [row[0] for row in rows]


def quarantine_types(lake: Lake) -> dict[str, int]:
    return dict(
        duckdb.sql(
            f"select error_type, count(*) from read_parquet('{lake.quarantine}/*/*.parquet') "
            "group by 1"
        ).fetchall()
    )


def test_messages_carry_the_capture_envelope() -> None:
    document = load_fixture("app-tls")
    capture = CaptureFlows("lab", "sha256:x", "app-tls.pcap", "complete", document["flows"])
    pairs = list(messages(capture))
    assert len(pairs) == 2
    key, value = pairs[0]
    assert key == b"lab|sha256:x"
    envelope = json.loads(value)
    assert envelope["contract_version"] == 1 and envelope["flow"] == document["flows"][0]


def test_bad_messages_are_quarantined_with_a_reason(lake: Lake) -> None:
    writer = BatchWriter(lake, "kafka-test", source="kafka", input_ref="topic")
    flow = load_fixture("app-tls")["flows"][0]
    add_message(writer, b"\xff not json")
    add_message(writer, json.dumps({"sensor_id": "lab", "flow": flow}).encode())
    add_message(
        writer,
        json.dumps(
            {
                "contract_version": 1,
                "sensor_id": "lab",
                "capture_id": "c",
                "completion_state": "complete",
                "flow": flow,
            }
        ).encode(),
    )
    result = writer.commit()
    assert result.records_written == 1
    assert result.quarantine_reasons == {"envelope_missing": 1, "invalid_json": 1}


def test_batch_ids_depend_only_on_the_offsets() -> None:
    a = kafka_batch_id("flows.v1", {1: [10, 20], 0: [5, 9]})
    assert a == kafka_batch_id("flows.v1", {0: [5, 9], 1: [10, 20]})
    assert a != kafka_batch_id("flows.v1", {0: [5, 9], 1: [10, 21]})
    assert kafka_batch_id("a/b", {0: [0, 0]}).startswith("kafka-a_b-")


kafka = pytest.mark.skipif(BOOTSTRAP is None, reason="FLOWLAKE_KAFKA_BOOTSTRAP is not set")


@pytest.fixture
def topic() -> str:
    return f"test.flows.{uuid.uuid4().hex[:8]}"


def consume(lake: Lake, topic: str, group: str, **kwargs: Any) -> list[Any]:
    from flowlake.streaming import BronzeConsumer

    consumer = BronzeConsumer(
        lake, bootstrap=BOOTSTRAP or "", topic=topic, group_id=group, **kwargs
    )
    consumer.run(idle_timeout=5.0)
    return consumer.results


@pytest.mark.kafka
@kafka
def test_streamed_flows_match_file_ingestion(tmp_path: Path, topic: str) -> None:
    from flowlake.streaming import ensure_topic, produce

    ensure_topic(BOOTSTRAP or "", topic, partitions=3)
    delivered = produce(capture_flows(CONFIG), bootstrap=BOOTSTRAP or "", topic=topic)

    streamed = Lake(tmp_path / "streamed")
    results = consume(streamed, topic, "group-a", batch_size=1_000, batch_seconds=1.0)
    assert sum(r.records_in for r in results) == delivered
    assert len(results) > 1, "small batches: the consumer must commit several micro-batches"

    # The same captures through the file path give the same record IDs and rejections.
    files = Lake(tmp_path / "files")
    truth = generate(CONFIG, tmp_path / "landing")
    ingest_path(files, tmp_path / "landing")
    assert record_ids(streamed) == record_ids(files)
    assert quarantine_types(streamed) == quarantine_types(files) == truth["expected_quarantine"]

    # Offsets were committed: the same group reads nothing more.
    assert consume(streamed, topic, "group-a") == []


@pytest.mark.kafka
@kafka
def test_a_replay_duplicates_bronze_but_not_the_gold_layer(tmp_path: Path, topic: str) -> None:
    from flowlake.evaluate import connect
    from flowlake.streaming import ensure_topic, produce
    from flowlake.transform import build

    ensure_topic(BOOTSTRAP or "", topic, partitions=2)
    produce(capture_flows(CONFIG), bootstrap=BOOTSTRAP or "", topic=topic)
    lake = Lake(tmp_path / "lake")
    consume(lake, topic, "group-1")
    unique = len(set(record_ids(lake)))
    # A new consumer group re-reads the whole topic, like a crash before the offset commit.
    consume(lake, topic, "group-2", batch_size=777)
    assert len(record_ids(lake)) == 2 * unique

    assert build(lake).success
    with connect(lake) as connection:
        assert connection.execute(
            "select count(*), count(distinct record_id) from gold.fct_flows"
        ).fetchone() == (unique, unique)
