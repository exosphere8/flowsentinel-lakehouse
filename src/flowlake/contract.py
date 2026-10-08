"""Data contract for FlowSentinel flow records (contract version 1).

The models mirror the JSON printed by ``flowsentinel flows --json``: a document with capture
metadata and a ``flows`` list of ``FlowRecord`` objects. They are the single source of truth for
the contract. The JSON Schemas in ``contracts/`` are generated from them, and the bronze writer
flattens validated records into the Arrow schema in :mod:`flowlake.bronze`.

Two kinds of rules live here:

* structural rules taken from FlowSentinel's Rust types: required fields, integer ranges
  (``u8``, ``u16``, ``u64``) and valid IP addresses;
* lakehouse rules that downstream models rely on: every flow has an event time, time does not
  run backwards, per-direction counters add up to the totals, and the addresses match the
  declared IP version.

Enum-like fields (TCP state, end reason, ...) accept any non-empty string, so a value added
upstream does not stop ingestion. dbt ``accepted_values`` tests report unknown values as
warnings instead: the "tolerant reader" pattern.
"""

from __future__ import annotations

import functools
import ipaddress
import json
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator
from pydantic_core import PydanticCustomError

CONTRACT_VERSION = 1

# FlowSentinel counters are u64; Parquet and DuckDB store them as signed 64-bit integers.
Count = Annotated[int, Field(ge=0, le=2**63 - 1)]
U8 = Annotated[int, Field(ge=0, le=255)]
U16 = Annotated[int, Field(ge=0, le=65_535)]
NonEmptyStr = Annotated[str, Field(min_length=1)]
FiniteFloat = Annotated[float, Field(allow_inf_nan=False)]
NonNegativeFloat = Annotated[float, Field(ge=0, allow_inf_nan=False)]

# The largest second that Python's datetime (year 9999) can represent.
_MAX_UNIX_SECONDS = 253_402_300_799


class _ContractModel(BaseModel):
    # Strict: "5" is not an integer and true is not a count. Unknown fields are ignored so that
    # fields added upstream do not stop ingestion; the upstream contract test reports them.
    model_config = ConfigDict(strict=True, extra="ignore", frozen=True)


class Timestamp(_ContractModel):
    unix_seconds: Annotated[int, Field(ge=0, le=_MAX_UNIX_SECONDS)]
    nanos: Annotated[int, Field(ge=0, le=999_999_999)]
    rfc3339: NonEmptyStr

    def sort_key(self) -> tuple[int, int]:
        return (self.unix_seconds, self.nanos)

    def to_datetime(self) -> datetime:
        """UTC datetime, truncated to microseconds (the precision of Parquet and DuckDB)."""
        return datetime.fromtimestamp(self.unix_seconds, tz=UTC) + timedelta(
            microseconds=self.nanos // 1_000
        )


class Endpoint(_ContractModel):
    ip: str
    port: U16

    @field_validator("ip")
    @classmethod
    def _normalize_ip(cls, value: str) -> str:
        parsed = _parse_ip(value)
        if parsed is None:
            raise PydanticCustomError(
                "invalid_ip_address", "{value!r} is not an IPv4 or IPv6 address", {"value": value}
            )
        return parsed[0]

    @property
    def version(self) -> int:
        parsed = _parse_ip(self.ip)
        assert parsed is not None  # validated on construction
        return parsed[1]


@functools.lru_cache(maxsize=65_536)
def _parse_ip(value: str) -> tuple[str, int] | None:
    """``(normalized address, IP version)``, or ``None`` if invalid. Cached: flows repeat IPs."""
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return None
    return str(address), address.version


class DirectionCounters(_ContractModel):
    packets: Count
    bytes: Count
    # Measured from decoded headers. On malformed packets it can exceed ``bytes``, so the
    # contract deliberately does not compare the two.
    payload_bytes: Count


class TcpSummary(_ContractModel):
    state: NonEmptyStr
    flags_initiator: list[NonEmptyStr]
    flags_responder: list[NonEmptyStr]
    syn_packets: Count
    fin_packets: Count
    rst_packets: Count
    duplicate_segments: Count


class ApplicationSummary(_ContractModel):
    protocols: list[NonEmptyStr] = Field(default_factory=list)
    dns_queries: list[str] = Field(default_factory=list)
    responder_dns_names: list[str] = Field(default_factory=list)
    http_hosts: list[str] = Field(default_factory=list)
    http_paths: list[str] = Field(default_factory=list)
    tls_server_names: list[str] = Field(default_factory=list)
    tls_alpn: list[str] = Field(default_factory=list)


class SizeSummary(_ContractModel):
    min: NonNegativeFloat
    max: NonNegativeFloat
    mean: NonNegativeFloat
    stddev: NonNegativeFloat
    median: NonNegativeFloat
    median_exact: bool


class InterArrivalSummary(_ContractModel):
    min_seconds: NonNegativeFloat
    max_seconds: NonNegativeFloat
    mean_seconds: NonNegativeFloat
    stddev_seconds: NonNegativeFloat


class FlowWarning(_ContractModel):
    code: NonEmptyStr
    count: Count


class FlowRecord(_ContractModel):
    """One finished, bidirectional flow, as produced by FlowSentinel's flow engine."""

    flow_id: Annotated[int, Field(ge=1, le=2**63 - 1)]
    ip_version: Literal[4, 6]
    protocol: U8
    protocol_name: str | None
    initiator: Endpoint
    responder: Endpoint
    initiator_basis: NonEmptyStr
    first_seen: Timestamp | None
    last_seen: Timestamp | None
    duration_seconds: NonNegativeFloat
    first_packet_index: Count
    last_packet_index: Count
    initiator_to_responder: DirectionCounters
    responder_to_initiator: DirectionCounters
    packets_total: Annotated[int, Field(ge=1, le=2**63 - 1)]
    bytes_total: Count
    packet_size: SizeSummary | None
    inter_arrival: InterArrivalSummary | None
    tcp: TcpSummary | None
    application: ApplicationSummary
    dominant_endpoint: NonEmptyStr
    end_reason: NonEmptyStr
    warnings: list[FlowWarning] = Field(default_factory=list)
    alert_ids: list[Count] = Field(default_factory=list)

    @model_validator(mode="after")
    def _lakehouse_rules(self) -> FlowRecord:
        if self.first_seen is None or self.last_seen is None:
            raise PydanticCustomError(
                "missing_event_time",
                "first_seen and last_seen are required: the lakehouse is organized by event time",
            )
        if self.last_seen.sort_key() < self.first_seen.sort_key():
            raise PydanticCustomError("time_reversed", "last_seen is earlier than first_seen")
        forward, backward = self.initiator_to_responder, self.responder_to_initiator
        if self.packets_total != forward.packets + backward.packets:
            raise PydanticCustomError(
                "totals_mismatch", "packets_total is not the sum of both directions"
            )
        if self.bytes_total != forward.bytes + backward.bytes:
            raise PydanticCustomError(
                "totals_mismatch", "bytes_total is not the sum of both directions"
            )
        if self.initiator.version != self.ip_version or self.responder.version != self.ip_version:
            raise PydanticCustomError(
                "ip_version_mismatch", "endpoint addresses do not match ip_version"
            )
        return self


class CaptureInfo(_ContractModel):
    """The parts of FlowSentinel's capture summary that the lakehouse keeps."""

    file_name: str | None = None
    packets_processed: Count | None = None


class FlowsDocument(_ContractModel):
    """The document printed by ``flowsentinel flows --json`` on success.

    ``flows`` is kept raw: each flow is validated on its own, so one bad record is quarantined
    instead of rejecting the whole capture.
    """

    capture: CaptureInfo
    completion_state: NonEmptyStr
    flows: list[Any]


class UpstreamError(_ContractModel):
    code: str
    category: str
    message: str


class UpstreamErrorDocument(_ContractModel):
    """The document FlowSentinel prints when it rejects a capture."""

    error: UpstreamError


class FlowMessage(_ContractModel):
    """A Kafka message on the flow topic: one flow plus where it came from.

    ``flow`` stays a raw dictionary for the same reason as in :class:`FlowsDocument`.
    """

    contract_version: Literal[1]
    sensor_id: NonEmptyStr
    capture_id: NonEmptyStr
    capture_file: str | None = None
    completion_state: NonEmptyStr
    flow: dict[str, Any]


def summarize_error(exc: ValidationError, limit: int = 1_000) -> tuple[str, str]:
    """Return ``(error_type, message)`` for a quarantine row.

    ``error_type`` is the first error's type (for example ``totals_mismatch`` or ``missing``),
    which makes quarantine counts easy to group. The message lists every error.
    """
    errors = exc.errors(include_url=False)
    first_type = str(errors[0]["type"]) if errors else "invalid"
    parts = []
    for error in errors:
        location = ".".join(str(part) for part in error["loc"]) or "<record>"
        parts.append(f"{location}: {error['msg']}")
    message = "; ".join(parts)
    if len(message) > limit:
        message = message[: limit - 3] + "..."
    return first_type, message


def json_schemas() -> dict[str, dict[str, Any]]:
    """The published JSON Schemas, keyed by file name."""
    flow_schema = FlowRecord.model_json_schema()
    flow_schema["$id"] = (
        "https://github.com/exosphere8/flowsentinel-lakehouse/contracts/flow_record.v1.schema.json"
    )
    flow_schema["description"] = (
        "A FlowSentinel flow record as accepted by the lakehouse (contract version 1)."
    )
    message_schema = FlowMessage.model_json_schema()
    message_schema["$id"] = (
        "https://github.com/exosphere8/flowsentinel-lakehouse/contracts/flow_message.v1.schema.json"
    )
    message_schema["description"] = (
        "A message on the flow topic. 'flow' must satisfy flow_record.v1.schema.json."
    )
    return {
        "flow_record.v1.schema.json": flow_schema,
        "flow_message.v1.schema.json": message_schema,
    }


def render_schema(schema: dict[str, Any]) -> str:
    return json.dumps(schema, indent=2, sort_keys=True) + "\n"
