"""The bronze layer: validated flow records as Hive-partitioned Parquet.

Layout under the lake root::

    bronze/flows/ingest_date=YYYY-MM-DD/<batch_id>.parquet       valid records
    bronze/quarantine/ingest_date=YYYY-MM-DD/<batch_id>.parquet  rejected records + reason
    _ledger/<batch_id>.json                                      one entry per finished batch

Guarantees:

* **Atomic files.** Each file is written to a hidden temporary name and renamed into place, so
  readers never see a half-written file.
* **Idempotent batches.** A batch ID is derived from its input (a content hash for files, the
  topic/partition/offset range for Kafka). The ledger entry is written last; a batch whose
  entry exists is skipped, and re-running a batch overwrites the same file names.
* **Deterministic record IDs.** ``record_id`` hashes (sensor, capture, flow ID), so the same
  flow gets the same ID however it arrives. Silver deduplicates on it, which makes delivery
  effectively-once even when a crash replays a batch.
* **Bronze is partitioned by ingestion date**, not event date: late-arriving data lands in
  today's partition and incremental models pick it up by ``ingested_at``.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import ValidationError

from flowlake.contract import CONTRACT_VERSION, FlowRecord, summarize_error

_STRINGS = pa.list_(pa.string())
_TIMESTAMP = pa.timestamp("us", tz="UTC")

BRONZE_FLOW_SCHEMA = pa.schema(
    [
        pa.field("record_id", pa.string(), nullable=False),
        pa.field("batch_id", pa.string(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("sensor_id", pa.string(), nullable=False),
        pa.field("capture_id", pa.string(), nullable=False),
        pa.field("capture_file", pa.string()),
        pa.field("capture_completion_state", pa.string(), nullable=False),
        pa.field("contract_version", pa.int16(), nullable=False),
        pa.field("ingested_at", _TIMESTAMP, nullable=False),
        pa.field("flow_id", pa.int64(), nullable=False),
        pa.field("ip_version", pa.int8(), nullable=False),
        pa.field("protocol", pa.int16(), nullable=False),
        pa.field("protocol_name", pa.string()),
        pa.field("initiator_ip", pa.string(), nullable=False),
        pa.field("initiator_port", pa.int32(), nullable=False),
        pa.field("responder_ip", pa.string(), nullable=False),
        pa.field("responder_port", pa.int32(), nullable=False),
        pa.field("initiator_basis", pa.string(), nullable=False),
        pa.field("first_seen", _TIMESTAMP, nullable=False),
        pa.field("last_seen", _TIMESTAMP, nullable=False),
        pa.field("duration_seconds", pa.float64(), nullable=False),
        pa.field("first_packet_index", pa.int64(), nullable=False),
        pa.field("last_packet_index", pa.int64(), nullable=False),
        pa.field("i2r_packets", pa.int64(), nullable=False),
        pa.field("i2r_bytes", pa.int64(), nullable=False),
        pa.field("i2r_payload_bytes", pa.int64(), nullable=False),
        pa.field("r2i_packets", pa.int64(), nullable=False),
        pa.field("r2i_bytes", pa.int64(), nullable=False),
        pa.field("r2i_payload_bytes", pa.int64(), nullable=False),
        pa.field("packets_total", pa.int64(), nullable=False),
        pa.field("bytes_total", pa.int64(), nullable=False),
        pa.field("pkt_size_min", pa.float64()),
        pa.field("pkt_size_max", pa.float64()),
        pa.field("pkt_size_mean", pa.float64()),
        pa.field("pkt_size_stddev", pa.float64()),
        pa.field("pkt_size_median", pa.float64()),
        pa.field("pkt_size_median_exact", pa.bool_()),
        pa.field("iat_min_seconds", pa.float64()),
        pa.field("iat_max_seconds", pa.float64()),
        pa.field("iat_mean_seconds", pa.float64()),
        pa.field("iat_stddev_seconds", pa.float64()),
        pa.field("tcp_state", pa.string()),
        pa.field("tcp_flags_initiator", _STRINGS),
        pa.field("tcp_flags_responder", _STRINGS),
        pa.field("tcp_syn_packets", pa.int64()),
        pa.field("tcp_fin_packets", pa.int64()),
        pa.field("tcp_rst_packets", pa.int64()),
        pa.field("tcp_duplicate_segments", pa.int64()),
        pa.field("app_protocols", _STRINGS, nullable=False),
        pa.field("dns_queries", _STRINGS, nullable=False),
        pa.field("responder_dns_names", _STRINGS, nullable=False),
        pa.field("http_hosts", _STRINGS, nullable=False),
        pa.field("http_paths", _STRINGS, nullable=False),
        pa.field("tls_server_names", _STRINGS, nullable=False),
        pa.field("tls_alpn", _STRINGS, nullable=False),
        pa.field("dominant_endpoint", pa.string(), nullable=False),
        pa.field("end_reason", pa.string(), nullable=False),
        pa.field(
            "flow_warnings",
            pa.list_(pa.struct([("code", pa.string()), ("count", pa.int64())])),
            nullable=False,
        ),
        pa.field("upstream_alert_ids", pa.list_(pa.int64()), nullable=False),
    ]
)

QUARANTINE_SCHEMA = pa.schema(
    [
        pa.field("batch_id", pa.string(), nullable=False),
        pa.field("record_index", pa.int64(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("sensor_id", pa.string(), nullable=False),
        pa.field("capture_id", pa.string()),
        pa.field("ingested_at", _TIMESTAMP, nullable=False),
        pa.field("error_type", pa.string(), nullable=False),
        pa.field("error_message", pa.string(), nullable=False),
        pa.field("raw_record", pa.string(), nullable=False),
    ]
)

# Raw records kept in quarantine are cut to this many characters.
_RAW_RECORD_LIMIT = 16_384
# Zero-row file that keeps each dataset readable before any batch has landed.
_SCHEMA_PARTITION = "ingest_date=1970-01-01"
_SCHEMA_FILE = "_schema.parquet"


@dataclass(frozen=True)
class Lake:
    """Paths of one lake. Everything lives under ``root``."""

    root: Path

    @property
    def bronze_flows(self) -> Path:
        return self.root / "bronze" / "flows"

    @property
    def quarantine(self) -> Path:
        return self.root / "bronze" / "quarantine"

    @property
    def ledger(self) -> Path:
        return self.root / "_ledger"

    @property
    def warehouse(self) -> Path:
        return self.root / "warehouse.duckdb"

    def ensure(self) -> Lake:
        """Create the layout and the zero-row schema files. Safe to call repeatedly."""
        for dataset, schema in (
            (self.bronze_flows, BRONZE_FLOW_SCHEMA),
            (self.quarantine, QUARANTINE_SCHEMA),
        ):
            target = dataset / _SCHEMA_PARTITION / _SCHEMA_FILE
            if not target.exists():
                _atomic_write_table(schema.empty_table(), target)
        self.ledger.mkdir(parents=True, exist_ok=True)
        return self

    def ledger_entry(self, batch_id: str) -> dict[str, Any] | None:
        path = self.ledger / f"{batch_id}.json"
        if not path.exists():
            return None
        entry: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
        return entry

    def ledger_entries(self) -> list[dict[str, Any]]:
        return [
            json.loads(path.read_text(encoding="utf-8"))
            for path in sorted(self.ledger.glob("*.json"))
        ]


@dataclass(frozen=True)
class RecordContext:
    """Where a record came from. Shared by every record of one capture."""

    sensor_id: str
    capture_id: str
    capture_file: str | None
    completion_state: str


@dataclass
class BatchResult:
    batch_id: str
    # "ingested"; "skipped" (already in the ledger); "rejected" (the input can never be
    # ingested, ledgered); "failed" (an operational error, not ledgered, retried next run);
    # "split" (a capture ingested as several parts, each with its own batch)
    status: str
    source: str
    input_ref: str
    records_in: int = 0
    records_written: int = 0
    records_quarantined: int = 0
    quarantine_reasons: dict[str, int] = field(default_factory=dict)
    files: list[str] = field(default_factory=list)
    ingested_at: str | None = None
    error: str | None = None


def record_id(sensor_id: str, capture_id: str, flow_id: int) -> str:
    """Deterministic 128-bit ID of a flow record, as 32 hex characters."""
    key = f"{sensor_id}\x1f{capture_id}\x1f{flow_id}".encode()
    return hashlib.sha256(key).hexdigest()[:32]


def flatten(flow: FlowRecord, ctx: RecordContext, *, batch_id: str, source: str) -> dict[str, Any]:
    """Turn a validated record into one bronze row (see ``BRONZE_FLOW_SCHEMA``).

    ``ingested_at`` is left empty; :meth:`BatchWriter.commit` stamps it.
    """
    assert flow.first_seen is not None
    assert flow.last_seen is not None
    size, iat, tcp, app = flow.packet_size, flow.inter_arrival, flow.tcp, flow.application
    forward, backward = flow.initiator_to_responder, flow.responder_to_initiator
    return {
        "record_id": record_id(ctx.sensor_id, ctx.capture_id, flow.flow_id),
        "batch_id": batch_id,
        "source": source,
        "sensor_id": ctx.sensor_id,
        "capture_id": ctx.capture_id,
        "capture_file": ctx.capture_file,
        "capture_completion_state": ctx.completion_state,
        "contract_version": CONTRACT_VERSION,
        "ingested_at": None,
        "flow_id": flow.flow_id,
        "ip_version": flow.ip_version,
        "protocol": flow.protocol,
        "protocol_name": flow.protocol_name,
        "initiator_ip": flow.initiator.ip,
        "initiator_port": flow.initiator.port,
        "responder_ip": flow.responder.ip,
        "responder_port": flow.responder.port,
        "initiator_basis": flow.initiator_basis,
        "first_seen": flow.first_seen.to_datetime(),
        "last_seen": flow.last_seen.to_datetime(),
        "duration_seconds": flow.duration_seconds,
        "first_packet_index": flow.first_packet_index,
        "last_packet_index": flow.last_packet_index,
        "i2r_packets": forward.packets,
        "i2r_bytes": forward.bytes,
        "i2r_payload_bytes": forward.payload_bytes,
        "r2i_packets": backward.packets,
        "r2i_bytes": backward.bytes,
        "r2i_payload_bytes": backward.payload_bytes,
        "packets_total": flow.packets_total,
        "bytes_total": flow.bytes_total,
        "pkt_size_min": size.min if size else None,
        "pkt_size_max": size.max if size else None,
        "pkt_size_mean": size.mean if size else None,
        "pkt_size_stddev": size.stddev if size else None,
        "pkt_size_median": size.median if size else None,
        "pkt_size_median_exact": size.median_exact if size else None,
        "iat_min_seconds": iat.min_seconds if iat else None,
        "iat_max_seconds": iat.max_seconds if iat else None,
        "iat_mean_seconds": iat.mean_seconds if iat else None,
        "iat_stddev_seconds": iat.stddev_seconds if iat else None,
        "tcp_state": tcp.state if tcp else None,
        "tcp_flags_initiator": tcp.flags_initiator if tcp else None,
        "tcp_flags_responder": tcp.flags_responder if tcp else None,
        "tcp_syn_packets": tcp.syn_packets if tcp else None,
        "tcp_fin_packets": tcp.fin_packets if tcp else None,
        "tcp_rst_packets": tcp.rst_packets if tcp else None,
        "tcp_duplicate_segments": tcp.duplicate_segments if tcp else None,
        "app_protocols": app.protocols,
        "dns_queries": app.dns_queries,
        "responder_dns_names": app.responder_dns_names,
        "http_hosts": app.http_hosts,
        "http_paths": app.http_paths,
        "tls_server_names": app.tls_server_names,
        "tls_alpn": app.tls_alpn,
        "dominant_endpoint": flow.dominant_endpoint,
        "end_reason": flow.end_reason,
        "flow_warnings": [{"code": w.code, "count": w.count} for w in flow.warnings],
        "upstream_alert_ids": flow.alert_ids,
    }


class BatchWriter:
    """Validates the records of one batch and writes them to bronze in one go.

    Usage: ``add()`` each record, then ``commit()``. Nothing is visible to readers until
    ``commit()`` has renamed the files into place; the ledger entry is written after them.
    """

    def __init__(
        self,
        lake: Lake,
        batch_id: str,
        *,
        source: str,
        input_ref: str,
        ingested_at: datetime | None = None,
    ) -> None:
        if not batch_id or any(ch in batch_id for ch in "/\\:") or batch_id.startswith("."):
            raise ValueError(f"unsafe batch id: {batch_id!r}")
        if ingested_at is not None and ingested_at.tzinfo is None:
            raise ValueError("ingested_at must be timezone-aware")
        self.lake = lake
        self.batch_id = batch_id
        self.source = source
        self.input_ref = input_ref
        # Stamped at commit unless fixed here (tests do that): the closer the stamp is to the
        # moment the files become visible, the smaller the window an incremental run can miss.
        self._fixed_ingested_at = ingested_at.astimezone(UTC) if ingested_at else None
        self._rows: list[dict[str, Any]] = []
        self._quarantined: list[dict[str, Any]] = []
        self._reasons: Counter[str] = Counter()
        self._records_in = 0

    def add(self, raw: Any, ctx: RecordContext) -> bool:
        """Validate one raw flow. Returns ``True`` if it was accepted."""
        index = self._records_in
        self._records_in += 1
        try:
            flow = FlowRecord.model_validate(raw)
        except ValidationError as exc:
            error_type, message = summarize_error(exc)
            self.quarantine(
                raw,
                error_type,
                message,
                sensor_id=ctx.sensor_id,
                capture_id=ctx.capture_id,
                index=index,
            )
            return False
        self._rows.append(flatten(flow, ctx, batch_id=self.batch_id, source=self.source))
        return True

    def quarantine(
        self,
        raw: Any,
        error_type: str,
        message: str,
        *,
        sensor_id: str,
        capture_id: str | None,
        index: int | None = None,
    ) -> None:
        """Record a rejected input (a flow, or a message whose envelope was invalid)."""
        if index is None:
            index = self._records_in
            self._records_in += 1
        self._reasons[error_type] += 1
        self._quarantined.append(
            {
                "batch_id": self.batch_id,
                "record_index": index,
                "source": self.source,
                "sensor_id": sensor_id,
                "capture_id": capture_id,
                "ingested_at": None,  # stamped at commit
                "error_type": error_type,
                "error_message": message,
                "raw_record": _raw_text(raw),
            }
        )

    def commit(self, **ledger_extra: Any) -> BatchResult:
        self.lake.ensure()
        ingested_at = self._fixed_ingested_at or datetime.now(UTC)
        for row in self._rows:
            row["ingested_at"] = ingested_at
        for row in self._quarantined:
            row["ingested_at"] = ingested_at
        partition = f"ingest_date={ingested_at.date().isoformat()}"
        files = []
        if self._rows:
            target = self.lake.bronze_flows / partition / f"{self.batch_id}.parquet"
            _atomic_write_table(pa.Table.from_pylist(self._rows, schema=BRONZE_FLOW_SCHEMA), target)
            files.append(str(target.relative_to(self.lake.root)))
        if self._quarantined:
            target = self.lake.quarantine / partition / f"{self.batch_id}.parquet"
            _atomic_write_table(
                pa.Table.from_pylist(self._quarantined, schema=QUARANTINE_SCHEMA), target
            )
            files.append(str(target.relative_to(self.lake.root)))
        result = BatchResult(
            batch_id=self.batch_id,
            status="ingested",
            source=self.source,
            input_ref=self.input_ref,
            records_in=self._records_in,
            records_written=len(self._rows),
            records_quarantined=len(self._quarantined),
            quarantine_reasons=dict(sorted(self._reasons.items())),
            files=files,
            ingested_at=ingested_at.isoformat(),
        )
        write_ledger(self.lake, result, **ledger_extra)
        return result


def write_ledger(lake: Lake, result: BatchResult, **extra: Any) -> None:
    lake.ledger.mkdir(parents=True, exist_ok=True)
    entry = asdict(result) | extra
    _atomic_write_bytes(
        (json.dumps(entry, indent=2, sort_keys=True) + "\n").encode(),
        lake.ledger / f"{result.batch_id}.json",
    )


def _raw_text(raw: Any) -> str:
    try:
        text = json.dumps(raw, sort_keys=True, default=str)
    except (TypeError, ValueError):
        text = repr(raw)
    return text if len(text) <= _RAW_RECORD_LIMIT else text[: _RAW_RECORD_LIMIT - 3] + "..."


def _temporary_sibling(target: Path) -> Path:
    # Hidden and without the .parquet suffix, so "*.parquet" globs never match it.
    return target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")


def _atomic_write_table(table: pa.Table, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = _temporary_sibling(target)
    try:
        pq.write_table(table, temporary, compression="zstd")
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_copy(source: Path, target: Path) -> None:
    """Copy a file so that readers see either the old or the new version, never a mix."""
    _atomic_write_bytes(source.read_bytes(), target)


def _atomic_write_bytes(data: bytes, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = _temporary_sibling(target)
    try:
        with temporary.open("wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)
