"""Deterministic synthetic network traffic in FlowSentinel's flow format.

Real captures are small or sensitive, so the demo, the benchmarks and the end-to-end tests run
on synthetic traffic: two sensors watching an office network for a few days. Each sensor emits
one ``flowsentinel flows --json`` document per hour, so the synthetic data takes exactly the
same ingestion path as real captures.

The traffic contains four labeled incidents (a port scan, C2 beaconing, DNS tunneling and a
large exfiltration), one IP address that changes hostname, and a few deliberately corrupt
records. ``_ground_truth.json`` lists all of them, so the tests can measure the detections'
precision and recall and check that quarantine catches exactly the corrupt records.

Addresses come from private (RFC 1918) and documentation (RFC 5737) ranges and names from
reserved domains (RFC 2606), so nothing points at a real host. The same seed always produces
the same bytes.
"""

from __future__ import annotations

import json
import math
import random
from collections import Counter, defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

SECOND = 1_000_000  # microseconds
HOUR = 3_600 * SECOND

TCP_HEADER = 66  # Ethernet 14 + IPv4 20 + TCP 32 (with the timestamp option)
UDP_HEADER = 42  # Ethernet 14 + IPv4 20 + UDP 8
ICMP_HEADER = 42  # Ethernet 14 + IPv4 20 + ICMP 8
MSS = 1_448

DNS_SERVER = "10.10.0.53"
DOMAIN_CONTROLLER = "10.10.0.10"
FILE_SERVER = "10.10.0.20"
GATEWAY = "10.10.0.1"
NTP_SERVER = "192.0.2.123"

SENSORS = (("sensor-hq", "10.20"), ("sensor-branch", "10.30"))


@dataclass(frozen=True)
class Service:
    name: str
    ip: str
    weight: float
    upload_median: float  # bytes of payload sent by the client
    download_median: float


SERVICES = (
    Service("www.example.com", "198.51.100.10", 5, 3_000, 250_000),
    Service("mail.example.org", "198.51.100.20", 4, 20_000, 150_000),
    Service("files.example.net", "198.51.100.30", 2, 300_000, 2_000_000),
    Service("crm.example.com", "198.51.100.40", 2, 15_000, 400_000),
    Service("chat.example.com", "198.51.100.70", 4, 5_000, 30_000),
    Service("api.example.net", "198.51.100.80", 3, 2_000, 20_000),
    Service("updates.example.org", "203.0.113.60", 1, 2_000, 20_000_000),
)
# 198.51.100.40 is reassigned halfway through: an IP whose hostname changes (SCD type 2).
REASSIGNED_IP = "198.51.100.40"
REASSIGNED_NAME = "status.example.org"

CLOSED = {
    "state": "closed",
    "flags_initiator": ["FIN", "SYN", "PSH", "ACK"],
    "flags_responder": ["FIN", "SYN", "PSH", "ACK"],
    "syn_packets": 2,
    "fin_packets": 2,
    "rst_packets": 0,
    "duplicate_segments": 0,
}
RESET = {
    "state": "reset",
    "flags_initiator": ["SYN", "RST", "PSH", "ACK"],
    "flags_responder": ["SYN", "PSH", "ACK"],
    "syn_packets": 2,
    "fin_packets": 0,
    "rst_packets": 1,
    "duplicate_segments": 0,
}

CORRUPTIONS = ("totals_mismatch", "missing_event_time", "invalid_ip_address", "greater_than_equal")


@dataclass(frozen=True)
class SyntheticConfig:
    seed: int = 7
    start: date = date(2026, 9, 28)  # a Monday
    days: int = 3
    workstations_per_sensor: int = 20
    corrupt_rate: float = 0.002
    incidents: bool = True

    def __post_init__(self) -> None:
        if not 1 <= self.days <= 366:
            raise ValueError("days must be between 1 and 366")
        if not 1 <= self.workstations_per_sensor <= 10_000:
            raise ValueError("workstations_per_sensor must be between 1 and 10000")
        if not 0 <= self.corrupt_rate <= 1:
            raise ValueError("corrupt_rate must be between 0 and 1")

    @property
    def start_us(self) -> int:
        midnight = datetime(self.start.year, self.start.month, self.start.day, tzinfo=UTC)
        return int(midnight.timestamp()) * SECOND


@dataclass
class Capture:
    sensor_id: str
    file_stem: str
    document: dict[str, Any]


@dataclass
class _Stats:
    flows: int = 0
    captures: int = 0
    corrupted: Counter[str] = field(default_factory=Counter)


def workstation_ip(prefix: str, index: int) -> str:
    return f"{prefix}.{1 + index // 200}.{10 + index % 200}"


def iso(us: int) -> str:
    return datetime.fromtimestamp(us // SECOND, tz=UTC).isoformat().replace("+00:00", "Z")


def timestamp(us: int) -> dict[str, Any]:
    seconds, micros = divmod(us, SECOND)
    stamp = datetime.fromtimestamp(seconds, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S")
    return {"unix_seconds": seconds, "nanos": micros * 1_000, "rfc3339": f"{stamp}.{micros:06d}Z"}


def build_flow(
    *,
    start: int,
    duration: int,
    protocol: int,
    src: str,
    sport: int,
    dst: str,
    dport: int,
    up_payload: int,
    down_payload: int,
    up_packets: int,
    down_packets: int,
    tcp: dict[str, Any] | None = None,
    app: dict[str, list[str]] | None = None,
    end_reason: str | None = None,
) -> dict[str, Any]:
    """One flow record shaped like FlowSentinel's output (without its ``flow_id``)."""
    header = {6: TCP_HEADER, 17: UDP_HEADER}.get(protocol, ICMP_HEADER)
    up_bytes = up_payload + up_packets * header
    down_bytes = down_payload + down_packets * header
    packets = up_packets + down_packets
    total = up_bytes + down_bytes
    mean = total / packets
    low = min(float(header), mean)
    high = max(mean, min(float(header + MSS), mean * 1.8))
    seconds = duration / SECOND
    if packets >= 2:
        gap = seconds / (packets - 1)
        inter_arrival: dict[str, float] | None = {
            "min_seconds": round(gap * 0.1, 6),
            "max_seconds": round(max(gap, min(seconds, gap * 4)), 6),
            "mean_seconds": round(gap, 6),
            "stddev_seconds": round(gap * 0.8, 6),
        }
    else:
        inter_arrival = None
    if up_bytes > 0.55 * total:
        dominant = "initiator"
    elif down_bytes > 0.55 * total:
        dominant = "responder"
    else:
        dominant = "balanced"
    application: dict[str, list[str]] = {
        "protocols": [],
        "dns_queries": [],
        "responder_dns_names": [],
        "http_hosts": [],
        "http_paths": [],
        "tls_server_names": [],
        "tls_alpn": [],
    }
    application.update(app or {})
    names = {1: "ICMP", 6: "TCP", 17: "UDP"}
    return {
        "ip_version": 4,
        "protocol": protocol,
        "protocol_name": names.get(protocol),
        "initiator": {"ip": src, "port": sport},
        "responder": {"ip": dst, "port": dport},
        "initiator_basis": "tcp_syn" if protocol == 6 else "first_packet",
        "first_seen": timestamp(start),
        "last_seen": timestamp(start + duration),
        "duration_seconds": seconds,
        "first_packet_index": 0,  # assigned when the capture is assembled
        "last_packet_index": 0,
        "initiator_to_responder": {
            "packets": up_packets,
            "bytes": up_bytes,
            "payload_bytes": up_payload,
        },
        "responder_to_initiator": {
            "packets": down_packets,
            "bytes": down_bytes,
            "payload_bytes": down_payload,
        },
        "packets_total": packets,
        "bytes_total": total,
        "packet_size": {
            "min": round(low, 3),
            "max": round(high, 3),
            "mean": round(mean, 3),
            "stddev": round((high - low) / 4, 3),
            "median": round((low + high) / 2, 3),
            "median_exact": packets <= 256,
        },
        "inter_arrival": inter_arrival,
        "tcp": dict(tcp) if tcp else None,
        "application": application,
        "dominant_endpoint": dominant,
        "end_reason": end_reason or ("tcp_finished" if protocol == 6 else "idle_timeout"),
        "warnings": [],
        "alert_ids": [],
    }


class SyntheticNetwork:
    """Generates the captures of one configuration. Iterate ``captures()`` once."""

    def __init__(self, config: SyntheticConfig) -> None:
        self.config = config
        self.stats = _Stats()
        self.incidents: list[dict[str, Any]] = []
        self._incident_flows: dict[tuple[str, int], list[tuple[int, dict[str, Any]]]] = defaultdict(
            list
        )
        hours = config.days * 24
        self._reassign_hour = (config.days // 2) * 24 if config.days >= 2 else None
        self.workstations = {
            sensor: [workstation_ip(prefix, i) for i in range(config.workstations_per_sensor)]
            for sensor, prefix in SENSORS
        }
        if config.incidents:
            self._plan_incidents()
        self._hours = hours

    # ----------------------------------------------------------------- public API

    def captures(self) -> Iterator[Capture]:
        for hour in range(self._hours):
            for sensor, _prefix in SENSORS:
                yield self._capture(sensor, hour)

    def ground_truth(self) -> dict[str, Any]:
        """Labels for the generated data. Complete once ``captures()`` is exhausted."""
        config = self.config
        reassignment = None
        if self._reassign_hour is not None:
            reassignment = {
                "ip": REASSIGNED_IP,
                "names": ["crm.example.com", REASSIGNED_NAME],
                "changed_at": iso(config.start_us + self._reassign_hour * HOUR),
            }
        return {
            "generator": "flowlake.sources.synthetic",
            "seed": config.seed,
            "start": config.start.isoformat(),
            "days": config.days,
            "workstations_per_sensor": config.workstations_per_sensor,
            "captures": self.stats.captures,
            "flows_generated": self.stats.flows,
            "incidents": self.incidents,
            "expected_quarantine": dict(sorted(self.stats.corrupted.items())),
            "hostname_reassignment": reassignment,
        }

    def write(self, out_dir: Path) -> dict[str, Any]:
        """Write every capture to ``out_dir/sensor=<id>/`` and the labels to ``_ground_truth``."""
        for capture in self.captures():
            target = out_dir / f"sensor={capture.sensor_id}" / f"{capture.file_stem}.json"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(serialize(capture.document))
        truth = self.ground_truth()
        (out_dir / "_ground_truth.json").write_text(
            json.dumps(truth, indent=2) + "\n", encoding="utf-8"
        )
        return truth

    # ----------------------------------------------------------------- captures

    def _rng(self, *parts: object) -> random.Random:
        return random.Random(":".join(str(part) for part in (self.config.seed, *parts)))

    def _capture(self, sensor: str, hour: int) -> Capture:
        config = self.config
        rng = self._rng("capture", sensor, hour)
        hour_start = config.start_us + hour * HOUR
        flows: list[tuple[int, dict[str, Any]]] = []
        for host in self.workstations[sensor]:
            flows.extend(self._workstation_hour(rng, host, hour_start))
        if sensor == "sensor-hq":
            flows.extend(self._ntp_hour(rng, hour_start))
        self._corrupt(rng, flows)
        flows.extend(self._incident_flows.get((sensor, hour), []))
        flows.sort(key=lambda item: item[0])

        records = []
        cursor = 0
        end_reasons: Counter[str] = Counter()
        for flow_id, (_start, flow) in enumerate(flows, start=1):
            packets = flow["packets_total"] if isinstance(flow["packets_total"], int) else 1
            flow["first_packet_index"] = cursor + 1
            flow["last_packet_index"] = cursor + max(packets, 1)
            cursor += max(packets, 1)
            end_reasons[flow["end_reason"]] += 1
            records.append({"flow_id": flow_id, **flow})
        self.stats.flows += len(records)
        self.stats.captures += 1

        stamp = datetime.fromtimestamp(hour_start // SECOND, tz=UTC).strftime("%Y%m%dT%H%MZ")
        stem = f"{sensor}-{stamp}"
        document = {
            "capture": {
                "file_name": f"{stem}.pcap",
                "format": "pcap",
                "packets_processed": cursor,
                "earliest_timestamp": timestamp(flows[0][0]) if flows else None,
                "latest_timestamp": timestamp(flows[-1][0]) if flows else None,
            },
            "completion_state": "complete",
            "capture_warnings": [],
            "flow_summary": {
                "flows_total": len(records),
                "flows_retained": len(records),
                "flows_not_retained": 0,
                "end_reasons": dict(sorted(end_reasons.items())),
            },
            "flows": records,
        }
        return Capture(sensor_id=sensor, file_stem=stem, document=document)

    def _corrupt(self, rng: random.Random, flows: list[tuple[int, dict[str, Any]]]) -> None:
        """Break a few normal flows on purpose; quarantine must catch exactly these."""
        if self.config.corrupt_rate <= 0:
            return
        for _start, flow in flows:
            if rng.random() >= self.config.corrupt_rate:
                continue
            kind = rng.choice(CORRUPTIONS)
            if kind == "totals_mismatch":
                flow["bytes_total"] += 1
            elif kind == "missing_event_time":
                flow["first_seen"] = None
            elif kind == "invalid_ip_address":
                flow["initiator"]["ip"] = "10.20.1.300"
            else:
                flow["initiator_to_responder"]["packets"] = -1
            self.stats.corrupted[kind] += 1

    # ----------------------------------------------------------------- normal traffic

    def _rate_per_hour(self, hour_start: int) -> float:
        moment = datetime.fromtimestamp(hour_start // SECOND, tz=UTC)
        weekday, hour = moment.weekday() < 5, moment.hour
        if weekday and 8 <= hour < 18:
            return 20.0
        if weekday and hour in (7, 18, 19):
            return 8.0
        if not weekday and 9 <= hour < 20:
            return 3.0
        return 1.0

    def _service_name(self, service: Service, at: int) -> str:
        if (
            service.ip == REASSIGNED_IP
            and self._reassign_hour is not None
            and at >= self.config.start_us + self._reassign_hour * HOUR
        ):
            return REASSIGNED_NAME
        return service.name

    def _workstation_hour(
        self, rng: random.Random, host: str, hour_start: int
    ) -> Iterator[tuple[int, dict[str, Any]]]:
        rate = self._rate_per_hour(hour_start) / HOUR
        at = hour_start + int(rng.expovariate(rate))
        while at < hour_start + HOUR:
            kind = rng.random()
            if kind < 0.80:
                yield from self._tls_session(rng, host, at)
            elif kind < 0.85:
                yield self._http_session(rng, host, at)
            elif kind < 0.95:
                yield self._tcp_internal(rng, host, at, FILE_SERVER, 445, 40_000, 400_000)
            elif kind < 0.98:
                yield self._tcp_internal(rng, host, at, DOMAIN_CONTROLLER, 389, 800, 2_500)
            else:
                yield self._ping(rng, host, at)
            at += int(rng.expovariate(rate)) + 1

    def _tls_session(
        self, rng: random.Random, host: str, at: int
    ) -> Iterator[tuple[int, dict[str, Any]]]:
        service = rng.choices(SERVICES, weights=[s.weight for s in SERVICES])[0]
        name = self._service_name(service, at)
        resolved = rng.random() < 0.5
        if resolved:
            yield at, dns_flow(rng, host, at, name)
            at += rng.randint(5_000, 40_000)
        up = _lognormal(rng, service.upload_median, 1.0, 300, 50_000_000)
        down = _lognormal(rng, service.download_median, 1.2, 300, 500_000_000)
        yield (
            at,
            tls_flow(
                rng,
                host,
                at,
                service.ip,
                name,
                up,
                down,
                resolved=resolved,
                reset=rng.random() < 0.03,
            ),
        )

    def _http_session(self, rng: random.Random, host: str, at: int) -> tuple[int, dict[str, Any]]:
        up = _lognormal(rng, 600, 0.5, 200, 20_000)
        down = _lognormal(rng, 40_000, 1.0, 300, 5_000_000)
        path = rng.choice(["/", "/index.html", "/news", "/img/logo.png"])
        flow = _tcp_flow(
            rng,
            host,
            at,
            "198.51.100.10",
            80,
            up,
            down,
            app={"protocols": ["http"], "http_hosts": ["www.example.com"], "http_paths": [path]},
        )
        return at, flow

    def _tcp_internal(
        self,
        rng: random.Random,
        host: str,
        at: int,
        server: str,
        port: int,
        up_median: float,
        down_median: float,
    ) -> tuple[int, dict[str, Any]]:
        up = _lognormal(rng, up_median, 1.0, 200, 50_000_000)
        down = _lognormal(rng, down_median, 1.0, 200, 50_000_000)
        return at, _tcp_flow(rng, host, at, server, port, up, down)

    def _ping(self, rng: random.Random, host: str, at: int) -> tuple[int, dict[str, Any]]:
        flow = build_flow(
            start=at,
            duration=rng.randint(500, 3_000),
            protocol=1,
            src=host,
            sport=0,
            dst=GATEWAY,
            dport=0,
            up_payload=56,
            down_payload=56,
            up_packets=1,
            down_packets=1,
        )
        return at, flow

    def _ntp_hour(
        self, rng: random.Random, hour_start: int
    ) -> Iterator[tuple[int, dict[str, Any]]]:
        """The domain controller syncs time every 1024 s (+ up to 4 s): periodic, but benign."""
        period = 1_024 * SECOND
        first = self.config.start_us
        index = max(0, math.ceil((hour_start - first) / period))
        while (at := first + index * period) < hour_start + HOUR:
            at += rng.randint(0, 4 * SECOND)
            yield (
                at,
                build_flow(
                    start=at,
                    duration=rng.randint(10_000, 40_000),
                    protocol=17,
                    src=DOMAIN_CONTROLLER,
                    sport=123,
                    dst=NTP_SERVER,
                    dport=123,
                    up_payload=48,
                    down_payload=48,
                    up_packets=1,
                    down_packets=1,
                ),
            )
            index += 1

    # ----------------------------------------------------------------- incidents

    def _plan_incidents(self) -> None:
        config = self.config
        hq, branch = self.workstations["sensor-hq"], self.workstations["sensor-branch"]
        last = len(hq) - 1
        day0 = config.start_us
        scan_day = day0 + min(1, config.days - 1) * 24 * HOUR
        last_day = day0 + (config.days - 1) * 24 * HOUR

        self._beaconing(hq[min(12, last)], day0 + 9 * HOUR, day0 + 21 * HOUR)
        self._port_scan(hq[min(7, last)], FILE_SERVER, scan_day + 14 * HOUR)
        self._dns_tunneling(branch[min(5, last)], day0 + 20 * HOUR)
        self._exfiltration(branch[min(9, last)], last_day + 2 * HOUR + 10 * 60 * SECOND)

    def _add_incident(
        self,
        *,
        rule_id: str,
        technique: str,
        sensor: str,
        src: str,
        dst: str,
        flows: list[tuple[int, dict[str, Any]]],
        description: str,
    ) -> None:
        starts = [start for start, _flow in flows]
        ends = [start + int(flow["duration_seconds"] * SECOND) for start, flow in flows]
        self.incidents.append(
            {
                "incident_id": f"INC-{len(self.incidents) + 1}",
                "rule_id": rule_id,
                "mitre_technique_id": technique,
                "sensor_id": sensor,
                "src_ip": src,
                "dst_ip": dst,
                "start": iso(min(starts)),
                "end": iso(max(ends)),
                "flows": len(flows),
                "description": description,
            }
        )
        for start, flow in flows:
            hour = (start - self.config.start_us) // HOUR
            self._incident_flows[(sensor, hour)].append((start, flow))

    def _beaconing(self, host: str, begin: int, end: int) -> None:
        rng = self._rng("incident", "beaconing")
        name, c2 = "cdn-sync.badactor.example", "203.0.113.66"
        flows: list[tuple[int, dict[str, Any]]] = []
        at, last_lookup_hour = begin, None
        while at < end:
            hour = at // HOUR
            if hour != last_lookup_hour:  # the implant re-resolves once per hour (TTL 3600)
                flows.append((at, dns_flow(rng, host, at, name)))
                last_lookup_hour = hour
            start = at + rng.randint(5_000, 20_000)
            flows.append(
                (
                    start,
                    tls_flow(
                        rng,
                        host,
                        start,
                        c2,
                        name,
                        rng.randint(400, 700),
                        rng.randint(200, 1_500),
                        resolved=True,
                    ),
                )
            )
            at += 300 * SECOND + rng.randint(-3 * SECOND, 3 * SECOND)
        tls_only = [item for item in flows if item[1]["protocol"] == 6]
        self._add_incident(
            rule_id="beaconing",
            technique="T1071.001",
            sensor="sensor-hq",
            src=host,
            dst=c2,
            flows=tls_only,
            description="Implant calls home over HTTPS every 300 s (+/- 3 s) for 12 hours.",
        )
        # The DNS lookups belong to the incident but are not what the rule detects.
        for start, flow in flows:
            if flow["protocol"] == 17:
                hour = (start - self.config.start_us) // HOUR
                self._incident_flows[("sensor-hq", hour)].append((start, flow))

    def _port_scan(self, host: str, target: str, begin: int) -> None:
        rng = self._rng("incident", "port_scan")
        ports = rng.sample(range(1, 1_025), 400)
        open_ports = {22, 80, 135, 139, 445}
        flows = []
        for index, port in enumerate(ports):
            at = begin + index * SECOND + rng.randint(0, 200_000)
            is_open = port in open_ports
            tcp = {
                "state": "reset",
                "flags_initiator": ["SYN", "RST"] if is_open else ["SYN"],
                "flags_responder": ["SYN", "ACK"] if is_open else ["RST", "ACK"],
                "syn_packets": 2 if is_open else 1,
                "fin_packets": 0,
                "rst_packets": 1,
                "duplicate_segments": 0,
            }
            flows.append(
                (
                    at,
                    build_flow(
                        start=at,
                        duration=rng.randint(200, 2_000),
                        protocol=6,
                        src=host,
                        sport=rng.randint(40_000, 60_000),
                        dst=target,
                        dport=port,
                        up_payload=0,
                        down_payload=0,
                        up_packets=2 if is_open else 1,
                        down_packets=1,
                        tcp=tcp,
                    ),
                )
            )
        self._add_incident(
            rule_id="port_scan",
            technique="T1046",
            sensor="sensor-hq",
            src=host,
            dst=target,
            flows=flows,
            description="SYN scan of 400 ports on the file server in under seven minutes.",
        )

    def _dns_tunneling(self, host: str, begin: int) -> None:
        rng = self._rng("incident", "dns_tunneling")
        alphabet = "abcdefghijklmnopqrstuvwxyz234567"
        flows = []
        for index in range(400):
            at = begin + index * 6 * SECOND + rng.randint(0, SECOND)
            label = "".join(rng.choice(alphabet) for _ in range(40))
            flows.append(
                (
                    at,
                    dns_flow(
                        rng, host, at, f"{label}.t.tunnel.example", answer=rng.randint(100, 250)
                    ),
                )
            )
        self._add_incident(
            rule_id="dns_tunneling",
            technique="T1071.004",
            sensor="sensor-branch",
            src=host,
            dst=DNS_SERVER,
            flows=flows,
            description="400 queries with 40-character random labels under tunnel.example.",
        )

    def _exfiltration(self, host: str, begin: int) -> None:
        rng = self._rng("incident", "exfiltration")
        name, drop = "drop.exfil.example", "203.0.113.99"
        flows: list[tuple[int, dict[str, Any]]] = []
        for index in range(3):
            at = begin + index * 15 * 60 * SECOND
            flows.append((at, dns_flow(rng, host, at, name)))
            start = at + rng.randint(5_000, 20_000)
            flows.append(
                (
                    start,
                    tls_flow(
                        rng,
                        host,
                        start,
                        drop,
                        name,
                        rng.randint(600_000_000, 1_200_000_000),
                        rng.randint(1_000_000, 3_000_000),
                        resolved=True,
                        duration=rng.randint(300, 600) * SECOND,
                    ),
                )
            )
        tls_only = [item for item in flows if item[1]["protocol"] == 6]
        self._add_incident(
            rule_id="exfiltration",
            technique="T1048",
            sensor="sensor-branch",
            src=host,
            dst=drop,
            flows=tls_only,
            description="About 2.7 GB uploaded to an unknown host at 02:10-02:50.",
        )
        for start, flow in flows:
            if flow["protocol"] == 17:
                hour = (start - self.config.start_us) // HOUR
                self._incident_flows[("sensor-branch", hour)].append((start, flow))


def dns_flow(
    rng: random.Random, host: str, at: int, name: str, *, answer: int | None = None
) -> dict[str, Any]:
    query = 12 + len(name) + 2 + 4  # header, encoded name, type and class
    return build_flow(
        start=at,
        duration=rng.randint(1_000, 30_000),
        protocol=17,
        src=host,
        sport=rng.randint(49_152, 65_535),
        dst=DNS_SERVER,
        dport=53,
        up_payload=query,
        down_payload=query + (answer or rng.randint(16, 64)),
        up_packets=1,
        down_packets=1,
        app={"protocols": ["dns"], "dns_queries": [name]},
    )


def tls_flow(
    rng: random.Random,
    host: str,
    at: int,
    server: str,
    name: str,
    up: int,
    down: int,
    *,
    resolved: bool,
    reset: bool = False,
    duration: int | None = None,
) -> dict[str, Any]:
    app = {
        "protocols": ["tls"],
        "tls_server_names": [name],
        "tls_alpn": ["h2", "http/1.1"],
        "responder_dns_names": [name] if resolved else [],
    }
    return _tcp_flow(rng, host, at, server, 443, up, down, app=app, reset=reset, duration=duration)


def _tcp_flow(
    rng: random.Random,
    host: str,
    at: int,
    server: str,
    port: int,
    up: int,
    down: int,
    *,
    app: dict[str, list[str]] | None = None,
    reset: bool = False,
    duration: int | None = None,
) -> dict[str, Any]:
    if duration is None:
        seconds = 0.05 + (up + down) / rng.uniform(2e6, 2e7) + rng.expovariate(0.5)
        duration = int(seconds * SECOND)
    return build_flow(
        start=at,
        duration=duration,
        protocol=6,
        src=host,
        sport=rng.randint(49_152, 65_535),
        dst=server,
        dport=port,
        up_payload=up,
        down_payload=down,
        up_packets=3 + math.ceil(up / MSS),
        down_packets=2 + math.ceil(down / MSS),
        tcp=RESET if reset else CLOSED,
        app=app,
    )


def _lognormal(rng: random.Random, median: float, sigma: float, low: int, high: int) -> int:
    return int(min(high, max(low, rng.lognormvariate(math.log(median), sigma))))


def serialize(document: dict[str, Any]) -> bytes:
    """The bytes of a capture document as written to disk. Its hash is the capture ID, so a
    capture has the same ID whether it arrives as a file or over Kafka."""
    return (json.dumps(document, separators=(",", ":")) + "\n").encode()


def generate(config: SyntheticConfig, out_dir: Path) -> dict[str, Any]:
    """Write a landing directory for ``config`` and return its ground truth."""
    return SyntheticNetwork(config).write(out_dir)


def default_start() -> date:
    """The Monday of the previous week: recent data for demos."""
    today = datetime.now(UTC).date()
    return today - timedelta(days=today.weekday() + 7)
