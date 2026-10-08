"""A static HTML dashboard over the gold layer.

One self-contained file (no scripts or styles from elsewhere), so it works from disk and on
GitHub Pages. Charts are SVG drawn here; a small script adds hover and keyboard tooltips.
Every chart also has a table view, and the page follows the reader's light or dark setting.
"""

from __future__ import annotations

import html
import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from flowlake import __version__
from flowlake.bronze import Lake
from flowlake.evaluate import Evaluation, evaluate

REPOSITORY = "https://github.com/exosphere8/flowsentinel-lakehouse"

# Direction -> categorical slot. Fixed, so a direction keeps its color whatever data is shown.
DIRECTIONS = ("outbound", "internal", "inbound", "external")
SEVERITY_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3}


def e(value: object) -> str:
    """Escape anything for HTML text or a quoted attribute."""
    return html.escape("" if value is None else str(value), quote=True)


def fmt_int(value: float | int) -> str:
    return f"{round(value):,}"


def fmt_compact(value: float) -> str:
    for limit, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(value) >= limit:
            return f"{value / limit:.1f}".rstrip("0").rstrip(".") + suffix
    return fmt_int(value)


def fmt_bytes(value: float) -> str:
    for limit, suffix in ((1e12, "TB"), (1e9, "GB"), (1e6, "MB"), (1e3, "kB")):
        if abs(value) >= limit:
            return f"{value / limit:.1f} {suffix}"
    return f"{int(value)} B"


def fmt_hour(moment: datetime) -> str:
    return moment.strftime("%a %d %b %H:%M")


def nice_ticks(maximum: float, count: int = 4) -> list[float]:
    """Round axis ticks from 0 up to at least ``maximum``."""
    if maximum <= 0:
        return [0.0, 1.0]
    raw = maximum / count
    magnitude = 10 ** math.floor(math.log10(raw))
    step = next(m * magnitude for m in (1, 2, 2.5, 5, 10) if m * magnitude >= raw)
    top = math.ceil(maximum / step) * step
    return [i * step for i in range(round(top / step) + 1)]


@dataclass
class Series:
    name: str
    slot: int  # 1-based categorical slot
    values: list[float]


# --------------------------------------------------------------------------- charts


def column_chart(
    labels: list[datetime],
    series: list[Series],
    *,
    value_format: str,
    chart_id: str,
    annotate_max: str | None = None,
) -> str:
    """Stacked columns over hourly buckets, with day ticks and per-column tooltips."""
    width, height = 960, 260
    left, right, top, bottom = 56, 12, 24, 40
    plot_w, plot_h = width - left - right, height - top - bottom
    totals = [sum(s.values[i] for s in series) for i in range(len(labels))]
    ticks = nice_ticks(max(totals, default=0))
    y_max = ticks[-1]
    slot_w = plot_w / max(len(labels), 1)
    bar_w = max(2.0, min(24.0, slot_w - 2))

    def y(value: float) -> float:
        return top + plot_h - value / y_max * plot_h

    fmt = fmt_bytes if value_format == "bytes" else fmt_compact
    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-labelledby="{chart_id}-t" '
        f'class="chart">'
    ]
    for tick in ticks:
        parts.append(
            f'<line class="grid" x1="{left}" x2="{width - right}" y1="{y(tick):.1f}" '
            f'y2="{y(tick):.1f}"/>'
        )
        parts.append(
            f'<text class="tick" x="{left - 8}" y="{y(tick) + 4:.1f}" '
            f'text-anchor="end">{e(fmt(tick))}</text>'
        )
    for i, moment in enumerate(labels):
        if moment.hour == 0:
            x = left + i * slot_w
            parts.append(
                f'<line class="axis" x1="{x:.1f}" x2="{x:.1f}" y1="{top + plot_h}" '
                f'y2="{top + plot_h + 6}"/>'
            )
            parts.append(
                f'<text class="tick" x="{x + 3:.1f}" y="{height - 12}">'
                f"{e(moment.strftime('%a %d %b'))}</text>"
            )
    parts.append(
        f'<line class="axis" x1="{left}" x2="{width - right}" y1="{top + plot_h}" '
        f'y2="{top + plot_h}"/>'
    )

    peak = max(range(len(labels)), key=lambda i: totals[i]) if labels else -1
    for i, moment in enumerate(labels):
        x = left + i * slot_w + (slot_w - bar_w) / 2
        base = 0.0
        drawn = [s for s in series if s.values[i] > 0]
        for index, item in enumerate(drawn):
            value = item.values[i]
            y_top, y_bottom = y(base + value), y(base)
            gap = 2 if index > 0 else 0  # surface gap between stacked segments
            h = max(0.0, y_bottom - y_top - gap)
            if h > 0:
                radius = min(4.0, bar_w / 2, h) if index == len(drawn) - 1 else 0.0
                parts.append(
                    f'<path class="s{item.slot}" d="{_column_path(x, y_top, bar_w, h, radius)}"/>'
                )
            base += value
        rows = [[s.name, fmt(s.values[i])] for s in series]
        if len(series) > 1:
            rows.append(["Total", fmt(totals[i])])
        tip = e(json.dumps({"title": fmt_hour(moment), "rows": rows}))
        parts.append(
            f'<rect class="hit" tabindex="0" x="{left + i * slot_w:.1f}" y="{top}" '
            f'width="{slot_w:.1f}" height="{plot_h}" data-tip="{tip}"/>'
        )
    if annotate_max and peak >= 0 and totals[peak] > 0:
        x = left + peak * slot_w + slot_w / 2
        anchor = "end" if x > width * 0.7 else "start"
        dx = -8 if anchor == "end" else 8
        parts.append(
            f'<text class="label" x="{x + dx:.1f}" y="{y(totals[peak]) + 4:.1f}" '
            f'text-anchor="{anchor}">{e(fmt(totals[peak]))} · {e(annotate_max)}</text>'
        )
    parts.append("</svg>")
    return f'<div class="chart-scroll">{"".join(parts)}</div>'


def _column_path(x: float, y: float, w: float, h: float, r: float) -> str:
    """A column with rounded top corners and a square base."""
    if r <= 0:
        return f"M{x:.1f},{y + h:.1f}V{y:.1f}H{x + w:.1f}V{y + h:.1f}Z"
    return (
        f"M{x:.1f},{y + h:.1f}V{y + r:.1f}Q{x:.1f},{y:.1f} {x + r:.1f},{y:.1f}"
        f"H{x + w - r:.1f}Q{x + w:.1f},{y:.1f} {x + w:.1f},{y + r:.1f}V{y + h:.1f}Z"
    )


def bar_chart(rows: list[tuple[str, float, str]], *, value_format: str, chart_id: str) -> str:
    """Horizontal bars (one series): label, value, secondary text for the tooltip."""
    fmt = fmt_bytes if value_format == "bytes" else fmt_compact
    width, row_h, label_w, value_w = 640, 30, 150, 90
    height = max(row_h * len(rows), row_h)
    plot_w = width - label_w - value_w
    maximum = max((value for _l, value, _s in rows), default=0) or 1
    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-labelledby="{chart_id}-t" '
        f'class="chart bars">'
    ]
    for i, (label, value, secondary) in enumerate(rows):
        y = i * row_h
        w = max(2.0, value / maximum * plot_w)
        bar_h = 16
        parts.append(
            f'<text class="cat" x="{label_w - 10}" y="{y + 19}" text-anchor="end">{e(label)}</text>'
        )
        parts.append(f'<path class="s1" d="{_bar_path(label_w, y + 7, w, bar_h, min(4.0, w))}"/>')
        parts.append(
            f'<text class="label" x="{label_w + w + 8:.1f}" y="{y + 19}">{e(fmt(value))}</text>'
        )
        tip = e(json.dumps({"title": label, "rows": [[secondary, fmt(value)]]}))
        parts.append(
            f'<rect class="hit" tabindex="0" x="0" y="{y}" width="{width}" '
            f'height="{row_h}" data-tip="{tip}"/>'
        )
    parts.append("</svg>")
    return f'<div class="chart-scroll">{"".join(parts)}</div>'


def _bar_path(x: float, y: float, w: float, h: float, r: float) -> str:
    """A bar with a rounded data end (right) and a square base (left)."""
    return (
        f"M{x:.1f},{y:.1f}H{x + w - r:.1f}Q{x + w:.1f},{y:.1f} {x + w:.1f},{y + r:.1f}"
        f"V{y + h - r:.1f}Q{x + w:.1f},{y + h:.1f} {x + w - r:.1f},{y + h:.1f}"
        f"H{x:.1f}Z"
    )


def data_table(headers: list[str], rows: list[list[str]], *, numeric: set[int]) -> str:
    head = "".join(
        f'<th class="{"num" if i in numeric else ""}">{e(h)}</th>' for i, h in enumerate(headers)
    )
    body = "".join(
        "<tr>"
        + "".join(
            f'<td class="{"num" if i in numeric else ""}">{e(cell)}</td>'
            for i, cell in enumerate(row)
        )
        + "</tr>"
        for row in rows
    )
    return (
        f'<div class="table-wrap"><table><thead><tr>{head}</tr></thead>'
        f"<tbody>{body}</tbody></table></div>"
    )


def legend(series: list[Series]) -> str:
    items = "".join(
        f'<span class="key"><span class="swatch s{s.slot}"></span>{e(s.name)}</span>'
        for s in series
    )
    return f'<div class="legend">{items}</div>'


# --------------------------------------------------------------------------- data


def _rows(connection: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    cursor = connection.execute(sql)
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def collect(lake: Lake) -> dict[str, Any]:
    connection = duckdb.connect(str(lake.warehouse), read_only=True)
    try:
        connection.execute("set TimeZone = 'UTC'")
        data: dict[str, Any] = {}
        data["kpis"] = _rows(
            connection,
            """
            select count(*) as flows, coalesce(sum(bytes_total), 0) as bytes,
                min(first_seen) as first_seen, max(last_seen) as last_seen,
                count(distinct sensor_id) as sensors
            from gold.fct_flows""",
        )[0]
        data["hosts"] = _rows(
            connection,
            """
            select count(*) filter (where is_internal) as internal,
                count(*) filter (where not is_internal) as external
            from gold.dim_hosts""",
        )[0]
        data["hourly"] = _rows(
            connection,
            """
            select hour_start, direction, sum(flows) as flows, sum(bytes) as bytes,
                sum(bytes_from_initiators) as uploaded
            from gold.agg_traffic_hourly group by all order by hour_start""",
        )
        data["alerts"] = _rows(
            connection,
            """
            select rule_id, rule_name, severity, mitre_technique_id, mitre_technique_name,
                mitre_tactic, src_ip, dst_ip, dst_port, dst_name, window_start, window_end,
                flows, bytes, evidence
            from gold.fct_alerts""",
        )
        data["talkers"] = _rows(
            connection,
            """
            select ip, zone, bytes_sent, flows_initiated, distinct_peers
            from gold.dim_hosts where is_internal
            order by bytes_sent desc limit 10""",
        )
        data["quarantine"] = _rows(
            connection,
            """
            select error_type, count(*) as records
            from silver.stg_flowsentinel__quarantine group by all order by records desc""",
        )
        data["batches"] = _rows(
            connection,
            """
            select count(*) as batches, coalesce(sum(records_accepted), 0) as accepted,
                coalesce(sum(records_quarantined), 0) as quarantined
            from gold.dq_batches""",
        )[0]
        data["renames"] = _rows(
            connection,
            """
            select ip, hostname, version, valid_from, valid_to, flows
            from gold.dim_host_names_scd2
            where ip in (select ip from gold.dim_host_names_scd2 group by ip having count(*) > 1)
            order by ip, version limit 20""",
        )
    finally:
        connection.close()
    data["alerts"].sort(key=lambda a: (SEVERITY_ORDER.get(a["severity"], 9), a["window_start"]))
    data["tests"] = _dbt_results(lake)
    data["ledger"] = lake.ledger_entries()
    return data


def _dbt_results(lake: Lake) -> dict[str, int]:
    path = lake.root / ".dbt" / "target" / "run_results.json"
    if not path.exists():
        return {}
    counts: dict[str, int] = {}
    for result in json.loads(path.read_text(encoding="utf-8")).get("results", []):
        unique_id = result.get("unique_id", "")
        if unique_id.startswith(("test.", "unit_test.")):
            status = str(result.get("status", "unknown"))
            counts[status] = counts.get(status, 0) + 1
    return counts


# --------------------------------------------------------------------------- page


def write_report(lake: Lake, out_dir: Path, *, ground_truth: dict[str, Any] | None = None) -> Path:
    """Write ``out_dir/index.html``. With ``ground_truth`` the page is labeled as synthetic
    data and shows the detections' scores."""
    data = collect(lake)
    scores = evaluate(lake, ground_truth) if ground_truth else None
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / "index.html"
    target.write_text(render(data, scores), encoding="utf-8")
    return target


def render(data: dict[str, Any], scores: Evaluation | None = None) -> str:
    kpis, hosts, batches = data["kpis"], data["hosts"], data["batches"]
    hours = _hour_axis(data["hourly"])
    sections = [
        _tiles(kpis, hosts, data["alerts"], batches),
        _traffic_section(data["hourly"], hours),
        _alerts_section(data["alerts"], scores),
        _talkers_section(data["talkers"]),
        _quality_section(data["quarantine"], batches, data["tests"], data["ledger"]),
        _renames_section(data["renames"]),
    ]
    window = ""
    if kpis["first_seen"] and kpis["last_seen"]:
        window = (
            f"{kpis['first_seen'].strftime('%d %b %Y %H:%M')} – "
            f"{kpis['last_seen'].strftime('%d %b %Y %H:%M')} UTC"
        )
    generated = datetime.now(UTC).strftime("%d %b %Y %H:%M UTC")
    return PAGE.format(
        css=CSS,
        script=SCRIPT,
        window=e(window),
        sensors=e(kpis["sensors"]),
        provenance="Synthetic demo data · " if scores is not None else "",
        generated=e(generated),
        version=e(__version__),
        repository=e(REPOSITORY),
        body="\n".join(sections),
    )


def _hour_axis(hourly: list[dict[str, Any]]) -> list[datetime]:
    if not hourly:
        return []
    first = min(row["hour_start"] for row in hourly).astimezone(UTC)
    last = max(row["hour_start"] for row in hourly).astimezone(UTC)
    count = int((last - first).total_seconds() // 3600) + 1
    return [first + timedelta(hours=i) for i in range(count)]


def _tile(label: str, value: str, note: str = "") -> str:
    note_html = f'<div class="tile-note">{e(note)}</div>' if note else ""
    return (
        f'<div class="tile"><div class="tile-label">{e(label)}</div>'
        f'<div class="tile-value">{e(value)}</div>{note_html}</div>'
    )


def _tiles(
    kpis: dict[str, Any],
    hosts: dict[str, Any],
    alerts: list[dict[str, Any]],
    batches: dict[str, Any],
) -> str:
    by_severity: dict[str, int] = {}
    for alert in alerts:
        by_severity[alert["severity"]] = by_severity.get(alert["severity"], 0) + 1
    severity_note = ", ".join(
        f"{count} {severity}"
        for severity, count in sorted(
            by_severity.items(), key=lambda i: SEVERITY_ORDER.get(i[0], 9)
        )
    )
    total = batches["accepted"] + batches["quarantined"]
    rate = batches["quarantined"] / total if total else 0.0
    return (
        '<section class="tiles" aria-label="Summary">'
        + _tile("Alerts", fmt_int(len(alerts)), severity_note or "none")
        + _tile("Flows", fmt_compact(kpis["flows"]), "deduplicated by record ID")
        + _tile("Traffic", fmt_bytes(kpis["bytes"]), "both directions")
        + _tile(
            "Hosts",
            fmt_int(hosts["internal"] + hosts["external"]),
            f"{fmt_int(hosts['internal'])} internal, {fmt_int(hosts['external'])} external",
        )
        + _tile(
            "Quarantined",
            f"{rate:.2%}",
            f"{fmt_int(batches['quarantined'])} of {fmt_int(total)} records",
        )
        + "</section>"
    )


def _traffic_section(hourly: list[dict[str, Any]], hours: list[datetime]) -> str:
    index = {moment: i for i, moment in enumerate(hours)}
    present = {row["direction"] for row in hourly}
    flows_series, outbound_bytes = [], [0.0] * len(hours)
    for slot, direction in enumerate(DIRECTIONS, start=1):
        if direction not in present:
            continue
        values = [0.0] * len(hours)
        for row in hourly:
            if row["direction"] == direction:
                position = index[row["hour_start"].astimezone(UTC)]
                values[position] += row["flows"]
                if direction == "outbound":
                    outbound_bytes[position] += row["uploaded"]
        flows_series.append(Series(direction.capitalize(), slot, values))
    flows_chart = column_chart(hours, flows_series, value_format="count", chart_id="flows")
    bytes_chart = column_chart(
        hours,
        [Series("Uploaded", 1, outbound_bytes)],
        value_format="bytes",
        chart_id="bytes",
        annotate_max="peak hour",
    )
    flow_rows = [
        [fmt_hour(moment)] + [fmt_int(s.values[i]) for s in flows_series]
        for i, moment in enumerate(hours)
    ]
    byte_rows = [[fmt_hour(moment), fmt_bytes(outbound_bytes[i])] for i, moment in enumerate(hours)]
    peak_note = "Bytes that internal hosts sent on connections they opened to external hosts."
    if any(outbound_bytes):
        peak = max(range(len(hours)), key=lambda i: outbound_bytes[i])
        typical = sorted(outbound_bytes)[len(outbound_bytes) // 2]
        peak_note += (
            f" Peak: {fmt_bytes(outbound_bytes[peak])} at {fmt_hour(hours[peak])}"
            f" (median hour: {fmt_bytes(typical)})."
        )
    return f"""
<section class="card">
  <h2 id="flows-t">Flows per hour, by direction</h2>
  <p class="sub">Every flow in the gold layer, stacked by direction. Hours are UTC.</p>
  {legend(flows_series)}
  {flows_chart}
  <details><summary>Show data table</summary>
  {
        data_table(
            ["Hour (UTC)"] + [s.name for s in flows_series],
            flow_rows,
            numeric=set(range(1, len(flows_series) + 1)),
        )
    }
  </details>
</section>
<section class="card">
  <h2 id="bytes-t">Bytes uploaded to external hosts per hour</h2>
  <p class="sub">{e(peak_note)}</p>
  {bytes_chart}
  <details><summary>Show data table</summary>
  {data_table(["Hour (UTC)", "Bytes uploaded"], byte_rows, numeric={1})}
  </details>
</section>"""


def _alert_summary(alert: dict[str, Any]) -> str:
    evidence = json.loads(alert["evidence"]) if alert["evidence"] else {}
    rule = alert["rule_id"]
    if rule == "port_scan":
        return (
            f"{evidence.get('distinct_ports')} ports probed, "
            f"{evidence.get('failed_ratio', 0):.1%} failed"
        )
    if rule == "beaconing":
        return (
            f"{evidence.get('connections')} connections every "
            f"{evidence.get('mean_interval_seconds')} s (CV {evidence.get('interval_cv')})"
        )
    if rule == "exfiltration":
        return (
            f"{fmt_bytes(evidence.get('bytes_out', 0))} out, "
            f"{evidence.get('upload_ratio', 0):.1%} upload"
        )
    if rule == "dns_tunneling":
        return (
            f"{evidence.get('distinct_names')} unique names, average label "
            f"{evidence.get('avg_label_length')} characters"
        )
    return ""


SEVERITY_ICONS = {"critical": "▲", "high": "◆", "medium": "●", "low": "○"}


def _alerts_section(alerts: list[dict[str, Any]], scores: Evaluation | None) -> str:
    rows = []
    for alert in alerts:
        severity = alert["severity"]
        target = alert["dst_ip"] or alert["dst_name"] or ""
        if alert["dst_ip"] and alert["dst_name"]:
            target = f"{alert['dst_ip']} ({alert['dst_name']})"
        if alert["dst_port"] is not None and alert["rule_id"] != "dns_tunneling":
            target += f":{alert['dst_port']}"
        hourly = alert["rule_id"] in ("port_scan", "dns_tunneling")
        window = (
            f"{alert['window_start'].strftime('%a %d %b %H:%M')} – "
            f"{alert['window_end'].strftime('%H:%M' if hourly else '%a %d %b')}"
        )
        rows.append(
            f'<tr><td><span class="sev sev-{e(severity)}"><span aria-hidden="true">'
            f"{SEVERITY_ICONS.get(severity, '●')}</span> {e(severity)}</span></td>"
            f'<td><strong>{e(alert["rule_name"])}</strong><div class="muted">'
            f"{e(alert['mitre_technique_id'])} {e(alert['mitre_technique_name'])}</div></td>"
            f'<td class="mono">{e(alert["src_ip"])} → {e(target)}</td>'
            f"<td>{e(window)}</td><td>{e(_alert_summary(alert))}</td></tr>"
        )
    table = (
        '<div class="table-wrap"><table class="alerts"><thead><tr><th>Severity</th>'
        "<th>Detection</th><th>Source → target</th><th>Window (UTC)</th><th>Evidence</th>"
        f"</tr></thead><tbody>{''.join(rows)}</tbody></table></div>"
        if rows
        else '<p class="sub">No alerts.</p>'
    )
    score_html = ""
    if scores is not None:
        score_rows = [
            [
                rule.rule_id,
                fmt_int(rule.incidents),
                fmt_int(rule.detected),
                fmt_int(rule.alerts),
                f"{rule.precision:.2f}",
                f"{rule.recall:.2f}",
            ]
            for rule in scores.rules.values()
        ]
        score_html = f"""
  <h3>Scored against the synthetic ground truth</h3>
  <p class="sub">The generator labels every incident it injects. Precision
  {scores.precision:.2f}, recall {scores.recall:.2f}; CI fails if either drops below 1.</p>
  {
            data_table(
                ["Rule", "Incidents", "Detected", "Alerts", "Precision", "Recall"],
                score_rows,
                numeric={1, 2, 3, 4, 5},
            )
        }"""
    return f"""
<section class="card">
  <h2>Alerts</h2>
  <p class="sub">SQL detections in dbt, mapped to MITRE ATT&amp;CK. Each row keeps its
  evidence so an analyst can verify it.</p>
  {table}{score_html}
</section>"""


def _talkers_section(talkers: list[dict[str, Any]]) -> str:
    rows = [
        (t["ip"], float(t["bytes_sent"]), f"{t['zone']}, {fmt_int(t['distinct_peers'])} peers")
        for t in talkers
    ]
    table_rows = [
        [
            t["ip"],
            t["zone"],
            fmt_bytes(t["bytes_sent"]),
            fmt_int(t["flows_initiated"]),
            fmt_int(t["distinct_peers"]),
        ]
        for t in talkers
    ]
    return f"""
<section class="card">
  <h2 id="talkers-t">Top internal hosts by bytes sent</h2>
  <p class="sub">The ten internal addresses that sent the most bytes, to any peer.</p>
  {bar_chart(rows, value_format="bytes", chart_id="talkers")}
  <details><summary>Show data table</summary>
  {
        data_table(
            ["Host", "Zone", "Bytes sent", "Flows started", "Peers"], table_rows, numeric={2, 3, 4}
        )
    }
  </details>
</section>"""


def _quality_section(
    quarantine: list[dict[str, Any]],
    batches: dict[str, Any],
    tests: dict[str, int],
    ledger: list[dict[str, Any]],
) -> str:
    rows = [(q["error_type"], float(q["records"]), "records quarantined") for q in quarantine]
    chart = (
        bar_chart(rows, value_format="count", chart_id="quarantine")
        if rows
        else '<p class="sub">No record failed the contract.</p>'
    )
    rejected = sum(1 for entry in ledger if entry.get("status") == "rejected")
    test_note = ", ".join(f"{count} {status}" for status, count in sorted(tests.items()))
    facts = [
        ("Batches ingested", fmt_int(batches["batches"])),
        ("Captures rejected by FlowSentinel", fmt_int(rejected)),
        ("Records accepted", fmt_int(batches["accepted"])),
        ("Records quarantined", fmt_int(batches["quarantined"])),
        ("dbt tests (last build)", test_note or "not run"),
    ]
    fact_html = "".join(f"<div><dt>{e(k)}</dt><dd>{e(v)}</dd></div>" for k, v in facts)
    return f"""
<section class="card">
  <h2 id="quarantine-t">Data quality</h2>
  <p class="sub">Every record is checked against the data contract on ingestion. Records that
  fail are kept in quarantine with the reason, never dropped.</p>
  <dl class="facts">{fact_html}</dl>
  <h3>Quarantined records by reason</h3>
  {chart}
</section>"""


def _renames_section(renames: list[dict[str, Any]]) -> str:
    if not renames:
        return ""
    rows = [
        [
            r["ip"],
            fmt_int(r["version"]),
            r["hostname"],
            r["valid_from"].strftime("%d %b %Y %H:%M"),
            r["valid_to"].strftime("%d %b %Y %H:%M") if r["valid_to"] else "current",
            fmt_int(r["flows"]),
        ]
        for r in renames
    ]
    return f"""
<section class="card">
  <h2>Hostname changes (SCD type 2)</h2>
  <p class="sub">Addresses that served a different name over time, rebuilt from event time
  with a gaps-and-islands query.</p>
  {
        data_table(
            ["IP", "Version", "Hostname", "Valid from (UTC)", "Valid to (UTC)", "Flows"],
            rows,
            numeric={1, 5},
        )
    }
</section>"""


CSS = """
:root {
  color-scheme: light;
  --page: #f9f9f7; --surface: #fcfcfb; --ink: #0b0b0b; --ink-2: #52514e; --muted: #898781;
  --grid: #e1e0d9; --axis: #c3c2b7; --border: rgba(11,11,11,0.10);
  --s1: #2a78d6; --s2: #eb6834; --s3: #1baf7a; --s4: #eda100;
  --critical: #d03b3b; --serious: #ec835a; --warning: #fab219; --good: #0ca30c;
}
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    color-scheme: dark;
    --page: #0d0d0d; --surface: #1a1a19; --ink: #ffffff; --ink-2: #c3c2b7; --muted: #898781;
    --grid: #2c2c2a; --axis: #383835; --border: rgba(255,255,255,0.10);
    --s1: #3987e5; --s2: #d95926; --s3: #199e70; --s4: #c98500;
  }
}
:root[data-theme="dark"] {
  color-scheme: dark;
  --page: #0d0d0d; --surface: #1a1a19; --ink: #ffffff; --ink-2: #c3c2b7; --muted: #898781;
  --grid: #2c2c2a; --axis: #383835; --border: rgba(255,255,255,0.10);
  --s1: #3987e5; --s2: #d95926; --s3: #199e70; --s4: #c98500;
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--page); color: var(--ink);
  font: 15px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif; }
main { max-width: 1040px; margin: 0 auto; padding: 32px 16px 48px; }
header h1 { font-size: 26px; margin: 0 0 4px; font-weight: 650; letter-spacing: -0.01em; }
header p { margin: 0; color: var(--ink-2); }
header .meta { color: var(--muted); font-size: 13px; margin-top: 6px; }
a { color: var(--s1); }
.tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 12px;
  margin: 24px 0; }
.tile, .card { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; }
.tile { padding: 14px 16px; }
.tile-label { color: var(--ink-2); font-size: 13px; }
.tile-value { font-size: 30px; font-weight: 600; margin-top: 2px; }
.tile-note { color: var(--muted); font-size: 12px; }
.card { padding: 20px; margin-top: 16px; }
.card h2 { font-size: 17px; margin: 0; font-weight: 600; }
.card h3 { font-size: 14px; margin: 20px 0 4px; font-weight: 600; }
.sub { color: var(--ink-2); margin: 2px 0 12px; font-size: 14px; }
.muted { color: var(--muted); font-size: 12px; }
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 13px; }
.chart-scroll { overflow-x: auto; }
.chart { width: 100%; min-width: 720px; height: auto; display: block; overflow: visible; }
.chart.bars { max-width: 640px; min-width: 480px; }
.chart .grid { stroke: var(--grid); stroke-width: 1; }
.chart .axis { stroke: var(--axis); stroke-width: 1; }
.chart .tick { fill: var(--muted); font-size: 11px; font-variant-numeric: tabular-nums; }
.chart .label { fill: var(--ink-2); font-size: 12px; }
.chart .cat { fill: var(--ink-2); font-size: 12px; font-family: ui-monospace, Menlo, monospace; }
.chart .hit { fill: transparent; cursor: default; outline: none; }
.chart .hit:hover, .chart .hit:focus-visible { fill: var(--ink); fill-opacity: 0.05; }
.s1 { fill: var(--s1); } .s2 { fill: var(--s2); } .s3 { fill: var(--s3); } .s4 { fill: var(--s4); }
.legend { display: flex; gap: 16px; font-size: 13px; color: var(--ink-2); margin-bottom: 6px; }
.key { display: inline-flex; align-items: center; gap: 6px; }
.swatch { width: 10px; height: 10px; border-radius: 2px; display: inline-block; }
.swatch.s1 { background: var(--s1); } .swatch.s2 { background: var(--s2); }
.swatch.s3 { background: var(--s3); } .swatch.s4 { background: var(--s4); }
details { margin-top: 10px; font-size: 13px; }
summary { cursor: pointer; color: var(--ink-2); }
.table-wrap { overflow-x: auto; margin-top: 8px; }
table { border-collapse: collapse; width: 100%; font-size: 13px; }
th, td { text-align: left; padding: 7px 10px; border-bottom: 1px solid var(--grid);
  vertical-align: top; }
th { color: var(--ink-2); font-weight: 600; }
td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
.sev { display: inline-flex; gap: 6px; align-items: center; font-weight: 600;
  text-transform: capitalize; white-space: nowrap; }
.sev span { font-size: 12px; }
.sev-critical span { color: var(--critical); } .sev-high span { color: var(--serious); }
.sev-medium span { color: var(--warning); } .sev-low span { color: var(--good); }
.facts { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 8px 16px;
  margin: 8px 0 0; }
.facts dt { color: var(--ink-2); font-size: 12px; } .facts dd { margin: 0; font-weight: 600; }
.tooltip { position: fixed; pointer-events: none; background: var(--surface); color: var(--ink);
  border: 1px solid var(--border); border-radius: 8px; padding: 8px 10px; font-size: 12px;
  box-shadow: 0 4px 16px rgba(0,0,0,0.12); display: none; z-index: 10; min-width: 140px; }
.tooltip .tt-title { color: var(--ink-2); margin-bottom: 4px; }
.tooltip .tt-row { display: flex; justify-content: space-between; gap: 16px; }
.tooltip .tt-row strong { font-variant-numeric: tabular-nums; }
footer { color: var(--muted); font-size: 13px; margin-top: 32px; }
"""

SCRIPT = """
(() => {
  const tip = document.createElement('div');
  tip.className = 'tooltip';
  tip.setAttribute('role', 'status');
  document.body.appendChild(tip);
  function show(target, x, y) {
    let data;
    try { data = JSON.parse(target.getAttribute('data-tip')); } catch (err) { return; }
    tip.replaceChildren();
    const title = document.createElement('div');
    title.className = 'tt-title';
    title.textContent = data.title;
    tip.appendChild(title);
    for (const [name, value] of data.rows) {
      const row = document.createElement('div');
      row.className = 'tt-row';
      const label = document.createElement('span');
      label.textContent = name;
      const strong = document.createElement('strong');
      strong.textContent = value;
      row.append(label, strong);
      tip.appendChild(row);
    }
    tip.style.display = 'block';
    const box = tip.getBoundingClientRect();
    const left = Math.min(x + 14, window.innerWidth - box.width - 8);
    const top = Math.min(y + 14, window.innerHeight - box.height - 8);
    tip.style.left = Math.max(8, left) + 'px';
    tip.style.top = Math.max(8, top) + 'px';
  }
  const hide = () => { tip.style.display = 'none'; };
  document.querySelectorAll('[data-tip]').forEach((el) => {
    el.addEventListener('pointermove', (ev) => show(el, ev.clientX, ev.clientY));
    el.addEventListener('pointerleave', hide);
    el.addEventListener('focus', () => {
      const r = el.getBoundingClientRect();
      show(el, r.left + r.width / 2, r.top);
    });
    el.addEventListener('blur', hide);
  });
})();
"""

PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>FlowSentinel Lakehouse</title>
<meta name="description"
  content="Network security dashboard built by the FlowSentinel Lakehouse pipeline.">
<style>{css}</style>
</head>
<body>
<main>
<header>
  <h1>FlowSentinel Lakehouse</h1>
  <p>Network flow telemetry from {sensors} sensor(s), {window}.</p>
  <div class="meta">{provenance}generated {generated} · flowlake {version} ·
  <a href="{repository}">source and pipeline</a></div>
</header>
{body}
<footer>Built from FlowSentinel flow records by a Python ingestion layer (data contract,
quarantine, idempotent Parquet), dbt models on DuckDB (silver and gold layers, SQL detections,
data tests) and this report. Times are UTC.</footer>
</main>
<script>{script}</script>
</body>
</html>
"""
