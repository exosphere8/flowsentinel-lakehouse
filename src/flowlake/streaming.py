"""The streaming path: flow records over Kafka (Redpanda) into bronze.

Producer: one message per flow, keyed by capture so a capture's flows stay ordered in one
partition. The value is a :class:`~flowlake.contract.FlowMessage` JSON document.

Consumer: reads micro-batches (up to ``batch_size`` messages or ``batch_seconds``), writes each
batch to bronze through the same :class:`~flowlake.bronze.BatchWriter` as file ingestion, and
commits the Kafka offsets only after the batch's files and ledger entry are in place.

Delivery is at-least-once: a crash between writing a batch and committing its offsets replays
those messages. Replays are harmless because record IDs are deterministic and ``fct_flows``
keeps one row per record ID, so the lake sees each flow effectively once. A message that is not
valid JSON or has an invalid envelope is quarantined, never dropped and never fatal.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

from flowlake.bronze import BatchResult, BatchWriter, Lake, RecordContext
from flowlake.contract import CONTRACT_VERSION, FlowMessage, summarize_error

DEFAULT_TOPIC = "flowsentinel.flows.v1"
DEFAULT_BOOTSTRAP = "localhost:19092"
DEFAULT_GROUP = "flowlake-bronze"


@dataclass(frozen=True)
class CaptureFlows:
    """The flows of one capture, ready to publish."""

    sensor_id: str
    capture_id: str
    capture_file: str | None
    completion_state: str
    flows: list[Any]


def messages(capture: CaptureFlows) -> Iterator[tuple[bytes, bytes]]:
    """``(key, value)`` pairs for every flow of a capture."""
    key = f"{capture.sensor_id}|{capture.capture_id}".encode()
    for flow in capture.flows:
        envelope = {
            "contract_version": CONTRACT_VERSION,
            "sensor_id": capture.sensor_id,
            "capture_id": capture.capture_id,
            "capture_file": capture.capture_file,
            "completion_state": capture.completion_state,
            "flow": flow,
        }
        yield key, json.dumps(envelope, separators=(",", ":")).encode()


def ensure_topic(bootstrap: str, topic: str, *, partitions: int = 3, timeout: float = 30.0) -> None:
    """Create the topic if it does not exist yet."""
    from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]

    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=timeout).topics:
        return
    futures = admin.create_topics([NewTopic(topic, num_partitions=partitions)])
    try:
        futures[topic].result(timeout=timeout)
    except Exception as exc:  # the topic may have been created concurrently
        if "TOPIC_ALREADY_EXISTS" not in str(exc):
            raise


def produce(
    captures: Iterable[CaptureFlows],
    *,
    bootstrap: str = DEFAULT_BOOTSTRAP,
    topic: str = DEFAULT_TOPIC,
    timeout: float = 60.0,
) -> int:
    """Publish every flow of ``captures``. Returns the number of messages delivered."""
    from confluent_kafka import KafkaException, Producer

    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,  # no duplicates from producer retries
            "acks": "all",
            "compression.type": "zstd",
            "linger.ms": 20,
        }
    )
    failures: list[str] = []
    delivered = 0

    def on_delivery(error: Any, _message: Any) -> None:
        nonlocal delivered
        if error is not None:
            failures.append(str(error))
        else:
            delivered += 1

    for capture in captures:
        for key, value in messages(capture):
            while True:
                try:
                    producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
                    break
                except BufferError:  # local queue full: let deliveries drain, then retry
                    producer.poll(0.5)
            producer.poll(0)
    remaining = producer.flush(timeout)
    if remaining:
        raise KafkaException(f"{remaining} messages were not delivered within {timeout:.0f} s")
    if failures:
        raise KafkaException(f"{len(failures)} messages failed: {failures[0]}")
    return delivered


class BronzeConsumer:
    """Consumes the flow topic into bronze in micro-batches."""

    def __init__(
        self,
        lake: Lake,
        *,
        bootstrap: str = DEFAULT_BOOTSTRAP,
        topic: str = DEFAULT_TOPIC,
        group_id: str = DEFAULT_GROUP,
        batch_size: int = 50_000,
        batch_seconds: float = 5.0,
    ) -> None:
        from confluent_kafka import Consumer

        self.lake = lake.ensure()
        self.topic = topic
        self.batch_size = batch_size
        self.batch_seconds = batch_seconds
        self.results: list[BatchResult] = []
        self._pending: list[Any] = []
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": group_id,
                "enable.auto.commit": False,  # offsets are committed after the batch is written
                "auto.offset.reset": "earliest",
                "isolation.level": "read_committed",
            }
        )
        self._consumer.subscribe([topic], on_revoke=self._on_revoke)

    def _on_revoke(self, _consumer: Any, _partitions: Any) -> None:
        # Flush before partitions move to another consumer, so it does not replay them.
        self.flush()

    def run(self, *, idle_timeout: float | None = None, max_batches: int | None = None) -> None:
        """Consume until ``idle_timeout`` seconds pass without a message or ``max_batches``
        batches are written. With neither, run until interrupted."""
        last_message = time.monotonic()
        batch_started = time.monotonic()
        try:
            while max_batches is None or len(self.results) < max_batches:
                records = self._consumer.consume(
                    num_messages=min(self.batch_size, 10_000), timeout=0.5
                )
                now = time.monotonic()
                for record in records:
                    if record.error():
                        from confluent_kafka import KafkaException

                        raise KafkaException(record.error())
                    if not self._pending:
                        batch_started = now
                    self._pending.append(record)
                if records:
                    last_message = now
                full = len(self._pending) >= self.batch_size
                due = bool(self._pending) and now - batch_started >= self.batch_seconds
                if full or due:
                    self.flush()
                elif idle_timeout is not None and now - last_message >= idle_timeout:
                    break
            self.flush()
        finally:
            self._consumer.close()

    def flush(self) -> BatchResult | None:
        """Write the pending messages as one batch, then commit their offsets."""
        if not self._pending:
            return None
        pending, self._pending = self._pending, []
        ranges: dict[int, list[int]] = {}
        for record in pending:
            first_last = ranges.setdefault(record.partition(), [record.offset(), record.offset()])
            first_last[0] = min(first_last[0], record.offset())
            first_last[1] = max(first_last[1], record.offset())
        batch_id = kafka_batch_id(self.topic, ranges)
        if self.lake.ledger_entry(batch_id) is not None:
            # Written before a crash that came before the offset commit: just commit.
            result = BatchResult(
                batch_id=batch_id, status="skipped", source="kafka", input_ref=self.topic
            )
        else:
            writer = BatchWriter(self.lake, batch_id, source="kafka", input_ref=self.topic)
            for record in pending:
                add_message(writer, record.value())
            result = writer.commit(
                topic=self.topic,
                offsets={str(p): r for p, r in sorted(ranges.items())},
            )
        from confluent_kafka import TopicPartition

        self._consumer.commit(
            offsets=[TopicPartition(self.topic, p, r[1] + 1) for p, r in sorted(ranges.items())],
            asynchronous=False,
        )
        self.results.append(result)
        return result


def add_message(writer: BatchWriter, value: bytes | None) -> None:
    """Validate one message's envelope and hand its flow to the writer (or quarantine it)."""
    try:
        document = json.loads(value or b"")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        text = (value or b"").decode(errors="replace")
        writer.quarantine(
            text,
            "invalid_json",
            f"message is not JSON: {exc}",
            sensor_id="unknown",
            capture_id=None,
        )
        return
    try:
        message = FlowMessage.model_validate(document)
    except ValidationError as exc:
        error_type, text = summarize_error(exc)
        sensor = document.get("sensor_id") if isinstance(document, dict) else None
        writer.quarantine(
            document,
            f"envelope_{error_type}",
            text,
            sensor_id=sensor if isinstance(sensor, str) and sensor else "unknown",
            capture_id=None,
        )
        return
    context = RecordContext(
        sensor_id=message.sensor_id,
        capture_id=message.capture_id,
        capture_file=message.capture_file,
        completion_state=message.completion_state,
    )
    writer.add(message.flow, context)


def kafka_batch_id(topic: str, ranges: dict[int, list[int]]) -> str:
    """Batch ID of a set of offset ranges: the same messages are always the same batch."""
    spec = ";".join(f"{p}:{r[0]}-{r[1]}" for p, r in sorted(ranges.items()))
    digest = hashlib.sha256(f"{topic}|{spec}".encode()).hexdigest()[:24]
    safe_topic = re.sub(r"[^A-Za-z0-9._-]", "_", topic)
    return f"kafka-{safe_topic}-{digest}"
