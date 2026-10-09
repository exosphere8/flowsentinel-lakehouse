"""pcapng conversion and capture splitting (flowlake.sources.pcap)."""

from __future__ import annotations

import json
import os
import stat
import struct
import sys
from pathlib import Path

import pytest

from flowlake.bronze import Lake
from flowlake.ingest import ingest_pcap
from flowlake.sources.pcap import CaptureFormatError, capture_format, prepare_capture

PCAPS = Path(__file__).parent / "fixtures" / "pcap"
FLOWS_MIXED = PCAPS / "flows-mixed.pcap"  # FlowSentinel fixture: 19 packets, microseconds


def read_pcap(path: Path) -> tuple[bytes, list[tuple[int, int, int, bytes]]]:
    """(global header, [(seconds, fraction, original length, data)]) of a classic pcap."""
    raw = path.read_bytes()
    endian = "<" if raw[:4] in (b"\xd4\xc3\xb2\xa1", b"\x4d\x3c\xb2\xa1") else ">"
    records, position = [], 24
    while position + 16 <= len(raw):
        seconds, fraction, incl, orig = struct.unpack(
            endian + "IIII", raw[position : position + 16]
        )
        records.append((seconds, fraction, orig, raw[position + 16 : position + 16 + incl]))
        position += 16 + incl
    return raw[:24], records


def write_pcapng(
    source: Path,
    target: Path,
    *,
    endian: str = "<",
    tsresol: int | None = None,
    obsolete_blocks: bool = False,
    interfaces: int = 1,
) -> None:
    """Rewrite a microsecond classic pcap as pcapng. Packet i goes to interface i % interfaces."""
    header, records = read_pcap(source)
    header_endian = "<" if header[:4] == b"\xd4\xc3\xb2\xa1" else ">"
    snap, link_type = struct.unpack(header_endian + "II", header[16:24])

    def block(block_type: int, body: bytes) -> bytes:
        body += b"\x00" * (-len(body) % 4)
        total = len(body) + 12
        return (
            struct.pack(endian + "II", block_type, total) + body + struct.pack(endian + "I", total)
        )

    out = block(0x0A0D0D0A, struct.pack(endian + "IHHq", 0x1A2B3C4D, 1, 0, -1))
    options = b""
    if tsresol is not None:
        options = struct.pack(endian + "HH", 9, 1) + bytes([tsresol]) + b"\x00" * 3
        options += struct.pack(endian + "HH", 0, 0)
    for _ in range(interfaces):
        out += block(0x00000001, struct.pack(endian + "HHI", link_type, 0, snap) + options)
    units = 10 ** (tsresol if tsresol is not None else 6)
    for index, (seconds, micros, orig, data) in enumerate(records):
        ts = seconds * units + micros * units // 1_000_000
        if obsolete_blocks:
            body = struct.pack(
                endian + "HHIIII", index % interfaces, 0, ts >> 32, ts & 0xFFFFFFFF, len(data), orig
            )
            out += block(0x00000002, body + data)
        else:
            body = struct.pack(
                endian + "IIIII", index % interfaces, ts >> 32, ts & 0xFFFFFFFF, len(data), orig
            )
            out += block(0x00000006, body + data)
    target.write_bytes(out)


def test_formats_are_detected_from_magic_bytes() -> None:
    assert capture_format(FLOWS_MIXED) == "pcap"
    assert capture_format(PCAPS / "be-nsec.pcap") == "pcap"
    assert capture_format(PCAPS / "minimal.pcapng") == "pcapng"
    assert capture_format(PCAPS / "invalid-magic.pcap") == "unknown"


def test_small_classic_pcaps_and_unknown_files_pass_through(tmp_path: Path) -> None:
    assert prepare_capture(FLOWS_MIXED, tmp_path).unchanged
    assert prepare_capture(PCAPS / "invalid-magic.pcap", tmp_path).unchanged
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("path", [FLOWS_MIXED, PCAPS / "be-nsec.pcap"])
def test_large_pcaps_are_split_without_losing_or_reordering_packets(
    path: Path, tmp_path: Path
) -> None:
    header, records = read_pcap(path)
    prepared = prepare_capture(path, tmp_path, max_packets=2)
    assert prepared.split and not prepared.converted
    assert len(prepared.parts) == -(-len(records) // 2)
    rejoined = []
    for part in prepared.parts:
        part_header, part_records = read_pcap(part)
        assert part_header == header  # same byte order, resolution and link type
        assert 1 <= len(part_records) <= 2
        rejoined += part_records
    assert rejoined == records


@pytest.mark.parametrize(
    ("endian", "tsresol", "obsolete"),
    [("<", None, False), (">", None, False), ("<", 9, False), ("<", None, True)],
)
def test_pcapng_converts_to_identical_packets(
    endian: str, tsresol: int | None, obsolete: bool, tmp_path: Path
) -> None:
    pcapng = tmp_path / "capture.pcapng"
    write_pcapng(FLOWS_MIXED, pcapng, endian=endian, tsresol=tsresol, obsolete_blocks=obsolete)
    prepared = prepare_capture(pcapng, tmp_path / "out")
    assert prepared.converted and len(prepared.parts) == 1
    header, records = read_pcap(prepared.parts[0])
    _, original = read_pcap(FLOWS_MIXED)
    nanoseconds = tsresol == 9
    assert header[:4] == (b"\x4d\x3c\xb2\xa1" if nanoseconds else b"\xd4\xc3\xb2\xa1")
    assert header[20:24] == read_pcap(FLOWS_MIXED)[0][20:24]  # the same link type
    scale = 1_000 if nanoseconds else 1
    assert records == [(s, f * scale, o, d) for s, f, o, d in original]


def test_pcapng_interfaces_become_separate_split_captures(tmp_path: Path) -> None:
    pcapng = tmp_path / "two.pcapng"
    write_pcapng(FLOWS_MIXED, pcapng, interfaces=2)
    prepared = prepare_capture(pcapng, tmp_path / "out", max_packets=4)
    _, original = read_pcap(FLOWS_MIXED)
    by_interface: dict[str, list[tuple[int, int, int, bytes]]] = {}
    for part in prepared.parts:
        interface = "i1" if ".s0i1." in part.name else "i0"
        by_interface.setdefault(interface, []).extend(read_pcap(part)[1])
    assert by_interface["i0"] == original[0::2]
    assert by_interface["i1"] == original[1::2]
    assert prepared.split


def test_a_pcapng_without_packets_becomes_an_empty_pcap(tmp_path: Path) -> None:
    prepared = prepare_capture(PCAPS / "minimal.pcapng", tmp_path)
    [part] = prepared.parts
    header, records = read_pcap(part)
    assert len(header) == 24 and records == []


@pytest.mark.parametrize(
    "corrupt",
    [
        lambda data: data[:-6],  # truncated final block
        lambda data: data[:4] + b"\xff\xff\xff\x7f" + data[8:],  # absurd block length
    ],
)
def test_corrupt_pcapng_is_reported(corrupt: object, tmp_path: Path) -> None:
    pcapng = tmp_path / "bad.pcapng"
    write_pcapng(FLOWS_MIXED, pcapng)
    pcapng.write_bytes(corrupt(pcapng.read_bytes()))  # type: ignore[operator]
    with pytest.raises(CaptureFormatError):
        prepare_capture(pcapng, tmp_path / "out")


def test_simple_packet_blocks_are_rejected_with_advice(tmp_path: Path) -> None:
    data = bytearray()
    shb = struct.pack("<IHHq", 0x1A2B3C4D, 1, 0, -1)
    data += struct.pack("<II", 0x0A0D0D0A, len(shb) + 12) + shb + struct.pack("<I", len(shb) + 12)
    idb = struct.pack("<HHI", 1, 0, 65535)
    data += struct.pack("<II", 1, len(idb) + 12) + idb + struct.pack("<I", len(idb) + 12)
    spb = struct.pack("<I", 4) + b"abcd"
    data += struct.pack("<II", 3, len(spb) + 12) + spb + struct.pack("<I", len(spb) + 12)
    pcapng = tmp_path / "simple.pcapng"
    pcapng.write_bytes(bytes(data))
    with pytest.raises(CaptureFormatError, match="enhanced packet blocks"):
        prepare_capture(pcapng, tmp_path / "out")


def checking_binary(tmp_path: Path, document: Path) -> Path:
    """A fake flowsentinel that fails unless it is handed a classic pcap."""
    script = tmp_path / "flowsentinel"
    script.write_text(
        f"#!{sys.executable}\n"
        "import sys\n"
        "path = sys.argv[sys.argv.index('--pcap') + 1]\n"
        "magic = open(path, 'rb').read(4)\n"
        "if magic not in (b'\\xd4\\xc3\\xb2\\xa1', b'\\xa1\\xb2\\xc3\\xd4',\n"
        "                 b'\\x4d\\x3c\\xb2\\xa1', b'\\xa1\\xb2\\x3c\\x4d'):\n"
        "    sys.exit(2)\n"
        f"sys.stdout.write(open({str(document)!r}).read())\n"
    )
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    return script


def test_pcapng_and_large_captures_are_ingested_part_by_part(
    lake: Lake, tmp_path: Path, fixtures_dir: Path
) -> None:
    binary = checking_binary(tmp_path, fixtures_dir / "flows-mixed.json")
    pcapng = tmp_path / "office.pcapng"
    write_pcapng(FLOWS_MIXED, pcapng)
    results = ingest_pcap(lake, pcapng, sensor_id="lab", binary=str(binary), max_packets=5)
    assert [r.status for r in results] == ["ingested"] * 4
    assert len({r.batch_id for r in results}) == 4
    assert all(r.input_ref.startswith(f"{pcapng}#part") for r in results)
    # The source is ledgered once every part is done: a re-run does not convert it again.
    [again] = ingest_pcap(lake, pcapng, sensor_id="lab", binary="/nonexistent", max_packets=5)
    assert again.status == "skipped"


def test_an_unreadable_capture_is_rejected_and_ledgered(lake: Lake, tmp_path: Path) -> None:
    pcapng = tmp_path / "bad.pcapng"
    write_pcapng(FLOWS_MIXED, pcapng)
    pcapng.write_bytes(pcapng.read_bytes()[:-6])
    [result] = ingest_pcap(lake, pcapng, sensor_id="lab", binary="/unused")
    assert result.status == "rejected" and "cannot read the capture" in (result.error or "")
    assert lake.ledger_entry(result.batch_id) is not None


@pytest.mark.upstream
@pytest.mark.skipif(not os.environ.get("FLOWSENTINEL_BIN"), reason="FLOWSENTINEL_BIN is not set")
def test_real_flowsentinel_sees_the_same_flows_in_pcapng_and_pcap(tmp_path: Path) -> None:
    from flowlake.sources.flowsentinel import run_flows_cli

    pcapng = tmp_path / "flows-mixed.pcapng"
    write_pcapng(FLOWS_MIXED, pcapng, tsresol=9)
    [converted] = prepare_capture(pcapng, tmp_path / "out").parts

    def flows(path: Path) -> list[tuple[object, ...]]:
        document = json.loads(run_flows_cli(path))
        return [
            (
                f["initiator"]["ip"],
                f["initiator"]["port"],
                f["responder"]["ip"],
                f["responder"]["port"],
                f["protocol"],
                f["packets_total"],
                f["bytes_total"],
                f["first_seen"]["unix_seconds"],
                f["first_seen"]["nanos"],
            )
            for f in document["flows"]
        ]

    assert flows(converted) == flows(FLOWS_MIXED)
