"""Prepare capture files for FlowSentinel: pcapng conversion and splitting.

FlowSentinel reads classic pcap files of up to one million packets per run. Real captures are
often pcapng (Wireshark's default) or larger than that, so before running the CLI each capture
goes through :func:`prepare_capture`:

* a classic pcap with at most ``max_packets`` records is used as is;
* a larger classic pcap is split into parts of at most ``max_packets`` records, each a valid
  pcap with the original header, so no packet is silently dropped at the CLI's limit;
* a pcapng file is converted to classic pcap, one output per interface (classic pcap has a
  single link type per file), and split the same way.

Everything streams block by block, so memory stays flat whatever the file size. Anything this
module does not understand (other formats, corrupt files) is passed through unchanged for
FlowSentinel to accept or reject with its own error.
"""

from __future__ import annotations

import struct
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO

# FlowSentinel's --max-packets ceiling.
MAX_PACKETS_PER_RUN = 1_000_000

_PCAP_MAGICS = {
    b"\xd4\xc3\xb2\xa1": ("<", False),  # little-endian, microseconds
    b"\xa1\xb2\xc3\xd4": (">", False),  # big-endian, microseconds
    b"\x4d\x3c\xb2\xa1": ("<", True),  # little-endian, nanoseconds
    b"\xa1\xb2\x3c\x4d": (">", True),  # big-endian, nanoseconds
}
_PCAPNG_SHB = 0x0A0D0D0A
_BYTE_ORDER_MAGIC = 0x1A2B3C4D
_IDB, _PB, _SPB, _EPB = 0x00000001, 0x00000002, 0x00000003, 0x00000006
# Sanity bounds: a block or packet larger than this is treated as corruption.
_MAX_BLOCK = 16 * 1024 * 1024
_MAX_PACKET = 262_144


class CaptureFormatError(ValueError):
    """The file looked like pcap or pcapng but could not be converted."""


@dataclass
class PreparedCapture:
    """The files to hand to FlowSentinel for one source capture."""

    source: Path
    parts: list[Path]
    converted: bool = False  # pcapng to pcap
    split: bool = False

    @property
    def unchanged(self) -> bool:
        return self.parts == [self.source]


def capture_format(path: Path) -> str:
    """``"pcap"``, ``"pcapng"`` or ``"unknown"``, from the first four bytes."""
    with path.open("rb") as handle:
        magic = handle.read(4)
    if magic in _PCAP_MAGICS:
        return "pcap"
    if len(magic) == 4 and struct.unpack("<I", magic)[0] == _PCAPNG_SHB:
        return "pcapng"
    return "unknown"


def prepare_capture(
    path: Path, workdir: Path, *, max_packets: int = MAX_PACKETS_PER_RUN
) -> PreparedCapture:
    """Return the classic-pcap part(s) to analyze for ``path``, written under ``workdir``."""
    if max_packets < 1:
        raise ValueError("max_packets must be at least 1")
    kind = capture_format(path)
    if kind == "pcapng":
        parts = _convert_pcapng(path, workdir, max_packets)
        return PreparedCapture(path, parts, converted=True, split=len(parts) > 1)
    if kind == "pcap":
        pieces = _split_pcap(path, workdir, max_packets)
        if pieces is None:
            return PreparedCapture(path, [path])
        return PreparedCapture(path, pieces, split=True)
    return PreparedCapture(path, [path])


# --------------------------------------------------------------------------- classic pcap


class _ChunkWriter:
    """Writes records to ``<stem>.partNNNN.pcap`` files of at most ``max_packets`` records."""

    def __init__(self, workdir: Path, stem: str, header: bytes, max_packets: int) -> None:
        self.workdir, self.stem, self.header, self.max_packets = workdir, stem, header, max_packets
        self.paths: list[Path] = []
        self._handle: BinaryIO | None = None
        self._count = 0

    def write(self, record: bytes) -> None:
        if self._handle is None or self._count >= self.max_packets:
            self._open_next()
        assert self._handle is not None
        self._handle.write(record)
        self._count += 1

    def _open_next(self) -> None:
        self.close()
        path = self.workdir / f"{self.stem}.part{len(self.paths) + 1:04d}.pcap"
        self.workdir.mkdir(parents=True, exist_ok=True)
        self._handle = path.open("wb")
        self._handle.write(self.header)
        self.paths.append(path)
        self._count = 0

    def finish(self) -> list[Path]:
        if not self.paths:  # no records at all: still one (empty) capture
            self._open_next()
        self.close()
        return self.paths

    def close(self) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None


def _iter_pcap_records(handle: BinaryIO, endian: str) -> Iterator[bytes]:
    """Yield raw records (16-byte header + data). Stops at a truncated record."""
    while True:
        header = handle.read(16)
        if len(header) < 16:
            return
        incl_len = struct.unpack(endian + "I", header[8:12])[0]
        if incl_len > _MAX_PACKET:
            raise CaptureFormatError(f"record of {incl_len} bytes")
        data = handle.read(incl_len)
        if len(data) < incl_len:
            return
        yield header + data


def _count_records_up_to(path: Path, endian: str, limit: int) -> int | None:
    """Number of records if it is at most ``limit``, else ``None``; ``-1`` if corrupt."""
    count = 0
    with path.open("rb") as handle:
        handle.seek(24)
        try:
            for _record in _iter_pcap_records(handle, endian):
                count += 1
                if count > limit:
                    return None
        except CaptureFormatError:
            return -1
    return count


def _split_pcap(path: Path, workdir: Path, max_packets: int) -> list[Path] | None:
    """Split a classic pcap into parts, or return ``None`` if it can be used as is."""
    with path.open("rb") as handle:
        header = handle.read(24)
    if len(header) < 24:
        return None  # truncated header: FlowSentinel reports it
    endian, _nanos = _PCAP_MAGICS[header[:4]]
    counted = _count_records_up_to(path, endian, max_packets)
    if counted is not None:  # small enough, or corrupt: hand it over unchanged
        return None
    writer = _ChunkWriter(workdir, path.stem, header, max_packets)
    with path.open("rb") as handle:
        handle.seek(24)
        for record in _iter_pcap_records(handle, endian):
            writer.write(record)
    return writer.finish()


# --------------------------------------------------------------------------- pcapng


@dataclass
class _Interface:
    link_type: int
    snap_length: int
    # Timestamp units per second and whether to write a nanosecond pcap.
    units_per_second: int = 1_000_000
    offset_seconds: int = 0
    writer: _ChunkWriter | None = field(default=None, repr=False)


def _convert_pcapng(path: Path, workdir: Path, max_packets: int) -> list[Path]:
    parts: list[Path] = []
    interfaces: list[_Interface] = []
    endian = "<"
    section = -1
    with path.open("rb") as handle:
        while True:
            head = handle.read(8)
            if not head:
                break
            if len(head) < 8:
                raise CaptureFormatError("truncated block header")
            # The section header's type reads the same in both byte orders; every other
            # block type is in the byte order of the current section.
            if head[:4] == b"\x0a\x0d\x0d\x0a":
                # The byte-order magic decides the endianness of this section.
                magic = handle.read(4)
                if len(magic) < 4:
                    raise CaptureFormatError("truncated section header")
                endian = "<" if struct.unpack("<I", magic)[0] == _BYTE_ORDER_MAGIC else ">"
                if struct.unpack(endian + "I", magic)[0] != _BYTE_ORDER_MAGIC:
                    raise CaptureFormatError("bad byte-order magic")
                length = struct.unpack(endian + "I", head[4:8])[0]
                body = magic + _read_body(handle, length, 12)
                for interface in interfaces:  # a new section starts new interfaces
                    if interface.writer:
                        parts.extend(interface.writer.finish())
                interfaces = []
                section += 1
                continue
            block_type, length = struct.unpack(endian + "II", head)
            body = _read_body(handle, length, 8)
            if block_type == _IDB:
                interfaces.append(_parse_idb(body, endian))
            elif block_type in (_EPB, _PB):
                _write_packet(
                    body, block_type, endian, interfaces, section, path, workdir, max_packets
                )
            elif block_type == _SPB:
                raise CaptureFormatError(
                    "simple packet blocks carry no timestamps; save the capture with "
                    "enhanced packet blocks"
                )
            # Other blocks (name resolution, statistics, custom) carry no packets.
    for interface in interfaces:
        if interface.writer:
            parts.extend(interface.writer.finish())
    if not parts:
        # Valid pcapng without packets: an empty capture for the first interface or Ethernet.
        link_type = interfaces[0].link_type if interfaces else 1
        writer = _ChunkWriter(workdir, path.stem, _pcap_header(link_type, 262_144, False), 1)
        parts = writer.finish()
    return parts


def _read_body(handle: BinaryIO, total_length: int, already_read: int) -> bytes:
    if total_length < 12 or total_length % 4 or total_length > _MAX_BLOCK:
        raise CaptureFormatError(f"invalid block length {total_length}")
    rest = handle.read(total_length - already_read)
    if len(rest) < total_length - already_read:
        raise CaptureFormatError("truncated block")
    return rest[:-4]  # drop the trailing copy of the block length


def _parse_idb(body: bytes, endian: str) -> _Interface:
    if len(body) < 8:
        raise CaptureFormatError("truncated interface description")
    link_type, _reserved, snap_length = struct.unpack(endian + "HHI", body[:8])
    interface = _Interface(link_type=link_type, snap_length=snap_length or 262_144)
    for code, value in _options(body[8:], endian):
        if code == 9 and value:  # if_tsresol
            resolution = value[0]
            if resolution & 0x80:
                interface.units_per_second = 2 ** (resolution & 0x7F)
            else:
                interface.units_per_second = 10**resolution
        elif code == 14 and len(value) >= 8:  # if_tsoffset, in seconds
            interface.offset_seconds = struct.unpack(endian + "q", value[:8])[0]
    if interface.units_per_second < 1 or interface.units_per_second > 10**18:
        raise CaptureFormatError("unsupported timestamp resolution")
    return interface


def _options(data: bytes, endian: str) -> Iterator[tuple[int, bytes]]:
    position = 0
    while position + 4 <= len(data):
        code, length = struct.unpack(endian + "HH", data[position : position + 4])
        if code == 0:  # opt_endofopt
            return
        value = data[position + 4 : position + 4 + length]
        yield code, value
        position += 4 + length + (-length % 4)


def _pcap_header(link_type: int, snap_length: int, nanoseconds: bool) -> bytes:
    magic = 0xA1B23C4D if nanoseconds else 0xA1B2C3D4
    return struct.pack("<IHHiIII", magic, 2, 4, 0, 0, snap_length, link_type)


def _write_packet(
    body: bytes,
    block_type: int,
    endian: str,
    interfaces: list[_Interface],
    section: int,
    path: Path,
    workdir: Path,
    max_packets: int,
) -> None:
    if block_type == _EPB:
        if len(body) < 20:
            raise CaptureFormatError("truncated enhanced packet block")
        interface_id, ts_high, ts_low, cap_len, orig_len = struct.unpack(
            endian + "IIIII", body[:20]
        )
    else:  # obsolete packet block
        if len(body) < 20:
            raise CaptureFormatError("truncated packet block")
        interface_id, _drops, ts_high, ts_low, cap_len, orig_len = struct.unpack(
            endian + "HHIIII", body[:20]
        )
    if interface_id >= len(interfaces):
        raise CaptureFormatError(f"packet for undeclared interface {interface_id}")
    if cap_len > _MAX_PACKET or 20 + cap_len > len(body):
        raise CaptureFormatError("packet longer than its block")
    interface = interfaces[interface_id]
    nanoseconds = interface.units_per_second > 1_000_000
    if interface.writer is None:
        stem = (
            path.stem
            if section == 0 and interface_id == 0
            else (f"{path.stem}.s{section}i{interface_id}")
        )
        header = _pcap_header(interface.link_type, interface.snap_length, nanoseconds)
        interface.writer = _ChunkWriter(workdir, stem, header, max_packets)
    units = (ts_high << 32) | ts_low
    seconds, remainder = divmod(units, interface.units_per_second)
    scale = 1_000_000_000 if nanoseconds else 1_000_000
    fraction = remainder * scale // interface.units_per_second
    seconds += interface.offset_seconds
    if not 0 <= seconds <= 0xFFFFFFFF:
        raise CaptureFormatError("timestamp outside the classic pcap range")
    record = struct.pack("<IIII", seconds, fraction, cap_len, orig_len)
    interface.writer.write(record + body[20 : 20 + cap_len])
