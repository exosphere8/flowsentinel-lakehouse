"""Measure the pipeline at scale and print a Markdown table.

    uv run python scripts/benchmark.py --workstations 300 --days 7 --workers 4 --dir /tmp/bench

Steps: generate synthetic captures, ingest them (contract check + Parquet), full dbt build,
an incremental build with nothing new, then one late capture and another incremental build.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import time
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import TypeVar

from flowlake.bronze import Lake
from flowlake.evaluate import connect, evaluate
from flowlake.ingest import discover, ingest_path
from flowlake.sources.synthetic import SyntheticConfig, generate
from flowlake.transform import build

T = TypeVar("T")


def timed(label: str, rows: list[tuple[str, float, str]], fn: Callable[[], T], note: str = "") -> T:
    start = time.perf_counter()
    result = fn()
    rows.append((label, time.perf_counter() - start, note))
    print(f"{label}: {rows[-1][1]:.1f} s", flush=True)
    return result


def node_timings(lake: Lake) -> list[tuple[str, float]]:
    results = json.loads((lake.root / ".dbt" / "target" / "run_results.json").read_text())
    pairs = [(r["unique_id"].split(".")[-1], r["execution_time"]) for r in results["results"]]
    return sorted(pairs, key=lambda pair: -pair[1])


def size(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workstations", type=int, default=300, help="per sensor")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--workers", type=int, default=os.cpu_count() or 1)
    parser.add_argument("--dir", type=Path, default=Path("bench"))
    args = parser.parse_args()

    shutil.rmtree(args.dir, ignore_errors=True)
    landing, late_dir, lake = args.dir / "landing", args.dir / "late", Lake(args.dir / "lake")
    config = SyntheticConfig(
        start=date(2026, 9, 28), days=args.days, workstations_per_sensor=args.workstations
    )
    rows: list[tuple[str, float, str]] = []

    truth = timed("Generate synthetic captures", rows, lambda: generate(config, landing))
    flows = truth["flows_generated"]
    # Hold one early capture back to arrive late.
    late = next(f for f in discover(landing) if "sensor-branch-20260928T0300Z" in f.name)
    (late_dir / late.parent.name).mkdir(parents=True)
    late = late.rename(late_dir / late.parent.name / late.name)

    timed(
        "Ingest (validate, quarantine, write Parquet)",
        rows,
        lambda: ingest_path(lake, landing, workers=args.workers),
        f"{args.workers} processes",
    )
    # The history above was loaded seconds ago, inside the default 30-minute lookback, so an
    # incremental run would re-read all of it. A lookback of 0 measures the steady state, where
    # earlier loads are older than the lookback and only new batches are read.
    steady = {"incremental_lookback_minutes": 0}
    assert timed("dbt build, full", rows, lambda: build(lake).success, "14 models, 52 tests")
    assert timed(
        "dbt build, incremental, no new data",
        rows,
        lambda: build(lake, variables=steady).success,
        "steady state",
    )
    timed("Ingest one late capture", rows, lambda: ingest_path(lake, late_dir))
    assert timed(
        "dbt build, incremental, one late capture",
        rows,
        lambda: build(lake, variables=steady).success,
        "steady state",
    )
    timings = node_timings(lake)

    evaluation = evaluate(lake, truth)
    with connect(lake) as connection:
        fact_rows = connection.execute("select count(*) from gold.fct_flows").fetchone()
    ingest_seconds = rows[1][1]
    print()
    print(
        f"Machine: {os.cpu_count()} vCPU, {platform.system()} {platform.machine()}, "
        f"Python {platform.python_version()}"
    )
    print(
        f"Data: {config.days} days, {2 * config.workstations_per_sensor} workstations, "
        f"{truth['captures']} captures, {flows:,} flows "
        f"({fact_rows[0] if fact_rows else 0:,} in fct_flows after quarantine)"
    )
    print(
        f"Landing JSON: {size(landing) / 1e6:,.0f} MB; bronze Parquet: "
        f"{size(lake.root / 'bronze') / 1e6:,.0f} MB; warehouse: "
        f"{lake.warehouse.stat().st_size / 1e6:,.0f} MB"
    )
    print(f"Ingest throughput: {flows / ingest_seconds:,.0f} flows/s")
    print(f"Detections: precision {evaluation.precision:.2f}, recall {evaluation.recall:.2f}")
    print()
    print("| Step | Seconds | Notes |")
    print("| --- | ---: | --- |")
    for label, seconds, note in rows:
        print(f"| {label} | {seconds:.1f} | {note} |")
    print()
    print(
        "Slowest dbt nodes in the last incremental run (seconds):",
        ", ".join(f"{name} {seconds:.2f}" for name, seconds in timings[:5]),
    )


if __name__ == "__main__":
    main()
