"""Consumer-driven contract test against the real FlowSentinel binary.

Needs FLOWSENTINEL_BIN (the `flowsentinel` CLI) and FLOWSENTINEL_FIXTURES (its fixtures/pcap
directory). CI builds FlowSentinel at a pinned commit and runs this, so a change upstream that
breaks the lakehouse fails here before it reaches a real pipeline.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from flowlake.contract import FlowRecord
from flowlake.sources.flowsentinel import UpstreamRejected, parse_document, run_flows_cli

BINARY = os.environ.get("FLOWSENTINEL_BIN")
FIXTURES = os.environ.get("FLOWSENTINEL_FIXTURES")

pytestmark = [
    pytest.mark.upstream,
    pytest.mark.skipif(
        not (BINARY and FIXTURES), reason="FLOWSENTINEL_BIN and FLOWSENTINEL_FIXTURES are not set"
    ),
]


def pcaps() -> list[Path]:
    return sorted(Path(FIXTURES or ".").glob("*.pcap"))


def test_every_flow_from_every_fixture_satisfies_the_contract() -> None:
    flows = 0
    rejected = 0
    for pcap in pcaps():
        try:
            capture = parse_document(run_flows_cli(pcap, binary=BINARY))
        except UpstreamRejected:
            rejected += 1
            continue
        for flow in capture.flows:
            FlowRecord.model_validate(flow)
            flows += 1
    assert flows > 0 and rejected > 0  # the fixtures include both good and bad captures


def test_upstream_fields_are_exactly_the_contract_fields() -> None:
    """A field added, renamed or removed upstream must be a deliberate contract change."""
    expected = set(FlowRecord.model_fields)
    for pcap in pcaps():
        document = json.loads(run_flows_cli(pcap, binary=BINARY))
        for flow in document.get("flows", []):
            assert set(flow) == expected, f"{pcap.name}: {set(flow) ^ expected}"
