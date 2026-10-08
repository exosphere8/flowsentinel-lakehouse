"""Score the detections and the quarantine against the synthetic ground truth.

An alert matches an incident when the rule and the source address are the same and their
time windows overlap. Precision is the share of alerts that match an incident; recall is the
share of incidents with at least one matching alert.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import duckdb

from flowlake.bronze import Lake


@dataclass
class RuleScore:
    rule_id: str
    incidents: int = 0
    detected: int = 0
    alerts: int = 0
    true_alerts: int = 0

    @property
    def precision(self) -> float:
        return self.true_alerts / self.alerts if self.alerts else 1.0

    @property
    def recall(self) -> float:
        return self.detected / self.incidents if self.incidents else 1.0


@dataclass
class Evaluation:
    rules: dict[str, RuleScore]
    missed_incidents: list[str]
    false_alerts: list[dict[str, Any]]
    expected_quarantine: dict[str, int]
    actual_quarantine: dict[str, int]
    notes: list[str] = field(default_factory=list)

    @property
    def precision(self) -> float:
        alerts = sum(rule.alerts for rule in self.rules.values())
        true_alerts = sum(rule.true_alerts for rule in self.rules.values())
        return true_alerts / alerts if alerts else 1.0

    @property
    def recall(self) -> float:
        incidents = sum(rule.incidents for rule in self.rules.values())
        detected = sum(rule.detected for rule in self.rules.values())
        return detected / incidents if incidents else 1.0

    @property
    def quarantine_matches(self) -> bool:
        return self.expected_quarantine == self.actual_quarantine

    @property
    def passed(self) -> bool:
        return self.precision == 1.0 and self.recall == 1.0 and self.quarantine_matches


def connect(lake: Lake) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(str(lake.warehouse), read_only=True)
    connection.execute("set TimeZone = 'UTC'")
    return connection


def evaluate(lake: Lake, truth: dict[str, Any]) -> Evaluation:
    with connect(lake) as connection:
        alerts = _rows(
            connection,
            "select alert_id, rule_id, src_ip, dst_ip, dst_name, window_start, window_end "
            "from gold.fct_alerts order by window_start, rule_id",
        )
        quarantine = dict(
            connection.execute(
                "select error_type, count(*) from silver.stg_flowsentinel__quarantine "
                "group by error_type"
            ).fetchall()
        )

    incidents = truth["incidents"]
    rules: dict[str, RuleScore] = {}
    for incident in incidents:
        rules.setdefault(incident["rule_id"], RuleScore(incident["rule_id"])).incidents += 1
    for alert in alerts:
        rules.setdefault(alert["rule_id"], RuleScore(alert["rule_id"])).alerts += 1

    detected: Counter[str] = Counter()
    false_alerts = []
    for alert in alerts:
        matching = [incident for incident in incidents if _matches(alert, incident)]
        if matching:
            rules[alert["rule_id"]].true_alerts += 1
            detected.update(incident["incident_id"] for incident in matching)
        else:
            false_alerts.append(alert)
    missed = []
    for incident in incidents:
        if detected[incident["incident_id"]]:
            rules[incident["rule_id"]].detected += 1
        else:
            missed.append(incident["incident_id"])

    return Evaluation(
        rules=dict(sorted(rules.items())),
        missed_incidents=missed,
        false_alerts=false_alerts,
        expected_quarantine={k: int(v) for k, v in truth["expected_quarantine"].items()},
        actual_quarantine={str(k): int(v) for k, v in sorted(quarantine.items())},
    )


def _matches(alert: dict[str, Any], incident: dict[str, Any]) -> bool:
    if alert["rule_id"] != incident["rule_id"] or alert["src_ip"] != incident["src_ip"]:
        return False
    start = datetime.fromisoformat(incident["start"])
    end = datetime.fromisoformat(incident["end"])
    window_start: datetime = alert["window_start"]
    window_end: datetime = alert["window_end"]
    return window_start <= end and start < window_end


def _rows(connection: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    cursor = connection.execute(sql)
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def format_evaluation(evaluation: Evaluation) -> str:
    lines = [
        f"{'rule':<15} {'incidents':>9} {'detected':>8} {'alerts':>6} {'precision':>9} "
        f"{'recall':>6}"
    ]
    for rule in evaluation.rules.values():
        lines.append(
            f"{rule.rule_id:<15} {rule.incidents:>9} {rule.detected:>8} {rule.alerts:>6} "
            f"{rule.precision:>9.2f} {rule.recall:>6.2f}"
        )
    lines.append(
        f"{'overall':<15} {'':>9} {'':>8} {'':>6} {evaluation.precision:>9.2f} "
        f"{evaluation.recall:>6.2f}"
    )
    status = "matches" if evaluation.quarantine_matches else "DOES NOT match"
    lines.append(
        f"quarantine {status} the injected corruptions: expected {evaluation.expected_quarantine}, "
        f"got {evaluation.actual_quarantine}"
    )
    if evaluation.missed_incidents:
        lines.append(f"missed incidents: {', '.join(evaluation.missed_incidents)}")
    for alert in evaluation.false_alerts:
        lines.append(
            f"false alert: {alert['rule_id']} {alert['src_ip']} -> "
            f"{alert['dst_ip'] or alert['dst_name']} at {alert['window_start']}"
        )
    return "\n".join(lines)
