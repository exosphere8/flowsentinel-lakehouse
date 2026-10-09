"""Batch ingestion of FlowSentinel documents and pcaps into bronze.

A landing directory may hold ``*.json`` documents printed by ``flowsentinel flows --json``
and captures (``*.pcap``, ``*.pcapng``, ``*.cap``), which are run through the FlowSentinel
CLI. The sensor that produced a file is taken from a ``sensor=<id>`` directory in its path,
unless one is given explicitly. Files and directories whose names start with ``_`` or ``.``
are ignored (for example ``_ground_truth.json``).
"""

from __future__ import annotations

import multiprocessing
import re
import tempfile
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, datetime
from itertools import repeat
from pathlib import Path

from flowlake.bronze import BatchResult, BatchWriter, Lake, RecordContext, write_ledger
from flowlake.sources.flowsentinel import (
    SourceError,
    parse_document,
    run_flows_cli,
    sha256_file,
    sha256_hex,
)
from flowlake.sources.pcap import MAX_PACKETS_PER_RUN, CaptureFormatError, prepare_capture

_SENSOR_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_CAPTURE_SUFFIXES = frozenset({".pcap", ".pcapng", ".cap"})
_SUFFIXES = _CAPTURE_SUFFIXES | {".json"}
DEFAULT_SENSOR = "default"


def validate_sensor_id(sensor_id: str) -> str:
    if not _SENSOR_ID.match(sensor_id):
        raise ValueError(
            f"invalid sensor id {sensor_id!r}: use 1-64 letters, digits, '.', '_' or '-'"
        )
    return sensor_id


def file_batch_id(sensor_id: str, content_sha256: str) -> str:
    """Batch ID of a file: the same file from the same sensor is one batch."""
    return f"file-{sha256_hex(f'{sensor_id}:{content_sha256}'.encode())[:32]}"


def ingest_document(
    lake: Lake,
    data: bytes,
    *,
    sensor_id: str,
    source: str,
    input_ref: str,
    content_sha256: str | None = None,
    force: bool = False,
    ingested_at: datetime | None = None,
) -> BatchResult:
    """Ingest one FlowSentinel flows document.

    ``content_sha256`` identifies the capture; it defaults to the hash of ``data``. For a pcap
    it is the hash of the pcap itself, so the capture ID does not depend on the CLI version.
    """
    validate_sensor_id(sensor_id)
    content_sha256 = content_sha256 or sha256_hex(data)
    batch_id = file_batch_id(sensor_id, content_sha256)
    if not force and (skipped := _skip_if_done(lake, batch_id, source, input_ref)):
        return skipped
    try:
        capture = parse_document(data)
    except SourceError as exc:
        return _reject(
            lake, batch_id, source, input_ref, str(exc), sensor_id, ingested_at or datetime.now(UTC)
        )
    context = RecordContext(
        sensor_id=sensor_id,
        capture_id=f"sha256:{content_sha256}",
        capture_file=capture.capture_file,
        completion_state=capture.completion_state,
    )
    writer = BatchWriter(
        lake, batch_id, source=source, input_ref=input_ref, ingested_at=ingested_at
    )
    for raw in capture.flows:
        writer.add(raw, context)
    return writer.commit(sensor_id=sensor_id, capture_id=context.capture_id)


def ingest_pcap(
    lake: Lake,
    pcap: Path,
    *,
    sensor_id: str,
    binary: str | None = None,
    force: bool = False,
    max_packets: int = MAX_PACKETS_PER_RUN,
) -> list[BatchResult]:
    """Run a capture through the FlowSentinel CLI and ingest the result.

    pcapng files are converted and captures with more than ``max_packets`` packets are split
    first (see :mod:`flowlake.sources.pcap`); each part is its own batch. The source file gets
    a ledger entry too once every part is done, so a re-run skips it without reading it again.
    """
    validate_sensor_id(sensor_id)
    try:
        content_sha256 = sha256_file(pcap)
    except OSError as exc:
        return [_unreadable(pcap, "flowsentinel_pcap", exc)]
    batch_id = file_batch_id(sensor_id, content_sha256)
    # Check the ledger before running the CLI: a finished capture is not decoded twice.
    if not force and (skipped := _skip_if_done(lake, batch_id, "flowsentinel_pcap", str(pcap))):
        return [skipped]
    with tempfile.TemporaryDirectory(prefix="flowlake-capture-") as scratch:
        try:
            prepared = prepare_capture(pcap, Path(scratch), max_packets=max_packets)
        except OSError as exc:  # unreadable now or no space to convert: try again next run
            return [_unreadable(pcap, "flowsentinel_pcap", exc)]
        except CaptureFormatError as exc:
            reason = f"cannot read the capture: {exc}"
            return [
                _reject(
                    lake,
                    batch_id,
                    "flowsentinel_pcap",
                    str(pcap),
                    reason,
                    sensor_id,
                    datetime.now(UTC),
                )
            ]
        if prepared.unchanged:
            return [
                _ingest_capture_file(
                    lake, pcap, content_sha256, str(pcap), sensor_id, binary, force=True
                )
            ]
        results = []
        for index, part in enumerate(prepared.parts, start=1):
            reference = f"{pcap}#part{index}"
            results.append(
                _ingest_capture_file(
                    lake, part, sha256_file(part), reference, sensor_id, binary, force=force
                )
            )
    if all(result.status != "failed" for result in results):
        summary = BatchResult(
            batch_id=batch_id,
            status="split",
            source="flowsentinel_pcap",
            input_ref=str(pcap),
            records_in=sum(r.records_in for r in results),
            records_written=sum(r.records_written for r in results),
            records_quarantined=sum(r.records_quarantined for r in results),
            ingested_at=datetime.now(UTC).isoformat(),
        )
        write_ledger(
            lake,
            summary,
            sensor_id=sensor_id,
            converted=prepared.converted,
            parts=[r.batch_id for r in results],
        )
    return results


def _ingest_capture_file(
    lake: Lake,
    capture: Path,
    content_sha256: str,
    input_ref: str,
    sensor_id: str,
    binary: str | None,
    *,
    force: bool,
) -> BatchResult:
    """Run one classic pcap through the CLI and ingest its flows."""
    batch_id = file_batch_id(sensor_id, content_sha256)
    if not force and (skipped := _skip_if_done(lake, batch_id, "flowsentinel_pcap", input_ref)):
        return skipped
    try:
        output = run_flows_cli(capture, binary=binary)
    except SourceError as exc:
        # Operational (binary missing, timeout, crash): not ledgered, so the next run retries.
        # A capture that FlowSentinel itself rejects is ledgered by ingest_document below.
        return BatchResult(
            batch_id=batch_id,
            status="failed",
            source="flowsentinel_pcap",
            input_ref=input_ref,
            error=str(exc),
        )
    return ingest_document(
        lake,
        output,
        sensor_id=sensor_id,
        source="flowsentinel_pcap",
        input_ref=input_ref,
        content_sha256=content_sha256,
        force=True,
    )


def ingest_path(
    lake: Lake,
    path: Path,
    *,
    sensor_id: str | None = None,
    binary: str | None = None,
    force: bool = False,
    workers: int = 1,
) -> list[BatchResult]:
    """Ingest a file, or every eligible file below a directory.

    Files are independent batches, so ``workers > 1`` ingests them in parallel processes.
    Results come back in sorted file order either way.
    """
    if sensor_id is not None:
        validate_sensor_id(sensor_id)
    lake.ensure()
    files = discover(path)
    sensors = [sensor_id or sensor_from_path(file) or DEFAULT_SENSOR for file in files]
    for sensor in sensors:
        validate_sensor_id(sensor)
    if workers <= 1 or len(files) <= 1:
        batches = [
            _ingest_file(lake, f, s, binary, force) for f, s in zip(files, sensors, strict=True)
        ]
    else:
        context = multiprocessing.get_context("spawn")
        with ProcessPoolExecutor(max_workers=workers, mp_context=context) as pool:
            batches = list(
                pool.map(_ingest_file, repeat(lake), files, sensors, repeat(binary), repeat(force))
            )
    return [result for batch in batches for result in batch]


def _ingest_file(
    lake: Lake, file: Path, sensor_id: str, binary: str | None, force: bool
) -> list[BatchResult]:
    if file.suffix.lower() in _CAPTURE_SUFFIXES:
        return ingest_pcap(lake, file, sensor_id=sensor_id, binary=binary, force=force)
    try:
        data = file.read_bytes()
    except OSError as exc:
        return [_unreadable(file, "flowsentinel_json", exc)]
    return [
        ingest_document(
            lake,
            data,
            sensor_id=sensor_id,
            source="flowsentinel_json",
            input_ref=str(file),
            force=force,
        )
    ]


def _unreadable(path: Path, source: str, exc: OSError) -> BatchResult:
    """A file that cannot be read right now, for example because of its permissions.

    It fails alone, without a ledger entry, so the other files are ingested and the next run
    tries it again.
    """
    return BatchResult(
        batch_id=f"unread-{sha256_hex(str(path).encode())[:32]}",
        status="failed",
        source=source,
        input_ref=str(path),
        error=f"cannot read the file: {exc.strerror or exc}",
    )


def discover(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    if not path.is_dir():
        raise FileNotFoundError(f"no such file or directory: {path}")
    found = []
    for candidate in path.rglob("*"):
        relative = candidate.relative_to(path)
        if any(part.startswith(("_", ".")) for part in relative.parts):
            continue
        if candidate.is_file() and candidate.suffix.lower() in _SUFFIXES:
            found.append(candidate)
    return sorted(found)


def sensor_from_path(path: Path) -> str | None:
    for part in reversed(path.parts[:-1]):
        if part.startswith("sensor="):
            return validate_sensor_id(part.removeprefix("sensor="))
    return None


def _skip_if_done(lake: Lake, batch_id: str, source: str, input_ref: str) -> BatchResult | None:
    entry = lake.ledger_entry(batch_id)
    if entry is None:
        return None
    return BatchResult(
        batch_id=batch_id,
        status="skipped",
        source=source,
        input_ref=input_ref,
        records_in=entry.get("records_in", 0),
        records_written=entry.get("records_written", 0),
        records_quarantined=entry.get("records_quarantined", 0),
        ingested_at=entry.get("ingested_at"),
    )


def _reject(
    lake: Lake,
    batch_id: str,
    source: str,
    input_ref: str,
    error: str,
    sensor_id: str,
    ingested_at: datetime,
) -> BatchResult:
    """Record a rejected input in the ledger so that it is visible and not retried forever."""
    result = BatchResult(
        batch_id=batch_id,
        status="rejected",
        source=source,
        input_ref=input_ref,
        ingested_at=ingested_at.astimezone(UTC).isoformat(),
        error=error,
    )
    write_ledger(lake, result, sensor_id=sensor_id)
    return result
