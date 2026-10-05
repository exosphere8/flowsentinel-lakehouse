from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from flowlake.contract import FlowRecord, json_schemas, render_schema, summarize_error

from .conftest import all_fixture_flows, load_fixture

CONTRACTS = Path(__file__).parents[1] / "contracts"


def first_flow() -> dict[str, Any]:
    flow: dict[str, Any] = load_fixture("flows-mixed")["flows"][0]
    return copy.deepcopy(flow)


def error_type(flow: dict[str, Any]) -> str:
    with pytest.raises(ValidationError) as caught:
        FlowRecord.model_validate(flow)
    return summarize_error(caught.value)[0]


def test_every_real_flowsentinel_flow_satisfies_the_contract() -> None:
    flows = all_fixture_flows()
    assert len(flows) == 85
    for flow in flows:
        FlowRecord.model_validate(flow)


def test_ipv6_addresses_are_normalized() -> None:
    flow = first_flow()
    flow["ip_version"] = 6
    flow["initiator"]["ip"] = "2001:DB8:0:0::A00"
    flow["responder"]["ip"] = "2001:db8::1400"
    record = FlowRecord.model_validate(flow)
    assert record.initiator.ip == "2001:db8::a00"


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        (lambda f: f.update(first_seen=None), "missing_event_time"),
        (lambda f: f.update(bytes_total=f["bytes_total"] + 1), "totals_mismatch"),
        (lambda f: f.update(packets_total=f["packets_total"] + 1), "totals_mismatch"),
        (lambda f: f["initiator"].update(ip="10.0.0.300"), "invalid_ip_address"),
        (lambda f: f["initiator_to_responder"].update(packets=-1), "greater_than_equal"),
        (lambda f: f["responder"].update(port=70000), "less_than_equal"),
        (lambda f: f.update(ip_version=6), "ip_version_mismatch"),
        (lambda f: f.update(last_seen=f["first_seen"] | {"unix_seconds": 1}), "time_reversed"),
        (lambda f: f.update(protocol="17"), "int_type"),  # strict: no string coercion
        (lambda f: f.update(duration_seconds=float("nan")), "finite_number"),
        (lambda f: f.pop("end_reason"), "missing"),
        (lambda f: f.update(end_reason=""), "string_too_short"),
    ],
)
def test_contract_violations_are_typed(change: Any, expected: str) -> None:
    flow = first_flow()
    change(flow)
    assert error_type(flow) == expected


def test_unknown_fields_and_enum_values_are_tolerated() -> None:
    flow = first_flow()
    flow["a_field_added_upstream"] = {"x": 1}
    flow["end_reason"] = "a_new_reason"
    assert FlowRecord.model_validate(flow).end_reason == "a_new_reason"


def test_summarize_error_lists_every_problem_and_truncates() -> None:
    flow = first_flow()
    flow["initiator"]["port"] = -1
    flow["responder"]["port"] = -2
    with pytest.raises(ValidationError) as caught:
        FlowRecord.model_validate(flow)
    error_type_, message = summarize_error(caught.value)
    assert error_type_ == "greater_than_equal"
    assert "initiator.port" in message and "responder.port" in message
    assert len(summarize_error(caught.value, limit=20)[1]) == 20


def test_published_json_schemas_are_up_to_date() -> None:
    for name, schema in json_schemas().items():
        assert (CONTRACTS / name).read_text(encoding="utf-8") == render_schema(schema), (
            f"{name} is stale: run `flowlake contract`"
        )
