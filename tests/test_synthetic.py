from __future__ import annotations

from collections import Counter
from datetime import date

import pytest
from pydantic import ValidationError

from flowlake.contract import FlowRecord, summarize_error
from flowlake.sources.synthetic import SyntheticConfig, SyntheticNetwork, serialize

from .conftest import SMALL


def documents(config: SyntheticConfig) -> list[bytes]:
    return [serialize(c.document) for c in SyntheticNetwork(config).captures()]


def test_the_same_seed_produces_the_same_bytes() -> None:
    config = SyntheticConfig(days=1, workstations_per_sensor=3)
    assert documents(config) == documents(config)
    assert documents(config) != documents(
        SyntheticConfig(seed=8, days=1, workstations_per_sensor=3)
    )


def test_corrupt_records_are_exactly_the_contract_failures() -> None:
    network = SyntheticNetwork(SMALL)
    failures: Counter[str] = Counter()
    flows = 0
    for capture in network.captures():
        for flow in capture.document["flows"]:
            flows += 1
            try:
                FlowRecord.model_validate(flow)
            except ValidationError as exc:
                failures[summarize_error(exc)[0]] += 1
    truth = network.ground_truth()
    assert truth["flows_generated"] == flows
    assert dict(failures) == truth["expected_quarantine"]
    assert set(failures) == {
        "totals_mismatch",
        "missing_event_time",
        "invalid_ip_address",
        "greater_than_equal",
    }


def test_ground_truth_lists_the_incidents_and_the_hostname_change() -> None:
    network = SyntheticNetwork(SMALL)
    for _ in network.captures():
        pass
    truth = network.ground_truth()
    assert sorted(i["rule_id"] for i in truth["incidents"]) == [
        "beaconing",
        "dns_tunneling",
        "exfiltration",
        "port_scan",
    ]
    assert truth["captures"] == SMALL.days * 24 * 2
    assert truth["hostname_reassignment"]["names"] == ["crm.example.com", "status.example.org"]


def test_one_day_has_no_hostname_change_and_incidents_can_be_disabled() -> None:
    network = SyntheticNetwork(
        SyntheticConfig(days=1, workstations_per_sensor=2, incidents=False, corrupt_rate=0)
    )
    for _ in network.captures():
        pass
    truth = network.ground_truth()
    assert truth["hostname_reassignment"] is None
    assert truth["incidents"] == [] and truth["expected_quarantine"] == {}


def test_capture_documents_look_like_flowsentinel_output() -> None:
    capture = next(SyntheticNetwork(SyntheticConfig(days=1, workstations_per_sensor=2)).captures())
    document = capture.document
    assert set(document) == {
        "capture",
        "completion_state",
        "capture_warnings",
        "flow_summary",
        "flows",
    }
    ids = [flow["flow_id"] for flow in document["flows"]]
    assert ids == list(range(1, len(ids) + 1))
    assert capture.file_stem == "sensor-hq-20260928T0000Z"


@pytest.mark.parametrize(
    "kwargs", [{"days": 0}, {"workstations_per_sensor": 0}, {"corrupt_rate": 1.5}]
)
def test_invalid_configs_are_rejected(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError):
        SyntheticConfig(start=date(2026, 9, 28), **kwargs)  # type: ignore[arg-type]
