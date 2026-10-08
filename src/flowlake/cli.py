"""``flowlake``: the command-line interface of the lakehouse."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
from collections import Counter
from collections.abc import Iterator, Sequence
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

from flowlake import __version__
from flowlake.bronze import BatchResult, Lake

if TYPE_CHECKING:
    from flowlake.sources.synthetic import SyntheticConfig
    from flowlake.streaming import CaptureFlows

DEFAULT_LAKE = "lake"


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "handler"):
        parser.print_help()
        return 2
    try:
        code: int = args.handler(args)
    except (FileNotFoundError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return code


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="flowlake",
        description="Security data lakehouse for FlowSentinel network flow telemetry.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "--lake",
        type=Path,
        default=Path(os.environ.get("FLOWLAKE_LAKE", DEFAULT_LAKE)),
        help="lake root directory (default: $FLOWLAKE_LAKE or ./lake)",
    )
    commands = parser.add_subparsers(title="commands", metavar="COMMAND")

    generate = commands.add_parser("generate", help="write synthetic FlowSentinel captures")
    _synthetic_options(generate)
    generate.add_argument(
        "--out", type=Path, default=Path("landing"), help="landing directory (default: ./landing)"
    )
    generate.set_defaults(handler=_generate)

    ingest = commands.add_parser("ingest", help="ingest FlowSentinel JSON documents or pcaps")
    ingest.add_argument("path", type=Path, help="a file or a directory to scan")
    ingest.add_argument("--sensor", help="sensor ID (default: from a sensor=<id> directory)")
    ingest.add_argument("--workers", type=int, default=1, help="parallel processes (default 1)")
    ingest.add_argument("--force", action="store_true", help="re-ingest finished batches")
    ingest.add_argument(
        "--flowsentinel-bin",
        help="flowsentinel binary for pcaps (default: $FLOWSENTINEL_BIN or PATH)",
    )
    ingest.set_defaults(handler=_ingest)

    transform = commands.add_parser("transform", help="run dbt build (models and tests)")
    transform.add_argument(
        "--full-refresh", action="store_true", help="rebuild incremental models from scratch"
    )
    transform.add_argument("--select", help="dbt node selection")
    transform.add_argument(
        "--vars",
        type=json.loads,
        help="dbt vars as JSON, for example '{\"beaconing_max_cv\": 0.1}'",
    )
    transform.add_argument(
        "--freshness", action="store_true", help="also check bronze source freshness"
    )
    transform.set_defaults(handler=_transform)

    evaluate = commands.add_parser(
        "evaluate", help="score detections against synthetic ground truth"
    )
    evaluate.add_argument("ground_truth", type=Path, help="the _ground_truth.json file")
    evaluate.set_defaults(handler=_evaluate)

    report = commands.add_parser("report", help="write the static HTML dashboard")
    report.add_argument(
        "--out", type=Path, default=Path("site"), help="output directory (default: ./site)"
    )
    report.add_argument("--ground-truth", type=Path, help="also show detection scores")
    report.set_defaults(handler=_report)

    demo = commands.add_parser("demo", help="generate, ingest, transform, evaluate and report")
    _synthetic_options(demo)
    demo.add_argument("--landing", type=Path, default=Path("landing"))
    demo.add_argument("--out", type=Path, default=Path("site"))
    demo.add_argument("--workers", type=int, default=os.cpu_count() or 1)
    demo.set_defaults(handler=_demo)

    contract = commands.add_parser("contract", help="write or check the JSON Schemas")
    contract.add_argument("--out", type=Path, default=Path("contracts"))
    contract.add_argument(
        "--check", action="store_true", help="fail if the files differ from the models"
    )
    contract.set_defaults(handler=_contract)

    stream = commands.add_parser("stream", help="the Kafka (Redpanda) path")
    stream_commands = stream.add_subparsers(title="stream commands", metavar="COMMAND")
    produce = stream_commands.add_parser("produce", help="publish flows to the topic")
    produce.add_argument(
        "path",
        type=Path,
        nargs="?",
        help="FlowSentinel JSON documents or pcaps (default: synthetic data)",
    )
    _synthetic_options(produce)
    _kafka_options(produce)
    produce.add_argument("--sensor", help="sensor ID for files (default: from the path)")
    produce.add_argument("--flowsentinel-bin")
    produce.set_defaults(handler=_produce)
    consume = stream_commands.add_parser("consume", help="consume the topic into bronze")
    _kafka_options(consume)
    consume.add_argument("--group", default="flowlake-bronze", help="consumer group")
    consume.add_argument("--batch-size", type=int, default=50_000)
    consume.add_argument("--batch-seconds", type=float, default=5.0)
    consume.add_argument(
        "--idle-timeout",
        type=float,
        help="stop after this many seconds without messages (default: run on)",
    )
    consume.set_defaults(handler=_consume)
    return parser


def _synthetic_options(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group("synthetic data")
    group.add_argument("--seed", type=int, default=7)
    group.add_argument(
        "--start",
        type=date.fromisoformat,
        default=date(2026, 9, 28),
        help="first day, YYYY-MM-DD (default 2026-09-28, a Monday)",
    )
    group.add_argument("--days", type=int, default=3)
    group.add_argument("--workstations", type=int, default=20, help="per sensor (default 20)")
    group.add_argument(
        "--corrupt-rate",
        type=float,
        default=0.002,
        help="share of normal flows corrupted on purpose (default 0.002)",
    )
    group.add_argument("--no-incidents", action="store_true")


def _kafka_options(parser: argparse.ArgumentParser) -> None:
    from flowlake.streaming import DEFAULT_TOPIC

    parser.add_argument(
        "--bootstrap",
        default=os.environ.get("FLOWLAKE_KAFKA_BOOTSTRAP", "localhost:19092"),
        help="Kafka bootstrap servers (default: $FLOWLAKE_KAFKA_BOOTSTRAP or localhost:19092)",
    )
    parser.add_argument("--topic", default=DEFAULT_TOPIC)


def _synthetic_config(args: argparse.Namespace) -> SyntheticConfig:
    from flowlake.sources.synthetic import SyntheticConfig

    return SyntheticConfig(
        seed=args.seed,
        start=args.start,
        days=args.days,
        workstations_per_sensor=args.workstations,
        corrupt_rate=args.corrupt_rate,
        incidents=not args.no_incidents,
    )


def _generate(args: argparse.Namespace) -> int:
    from flowlake.sources.synthetic import generate

    truth = generate(_synthetic_config(args), args.out)
    print(
        f"wrote {truth['captures']} captures with {truth['flows_generated']:,} flows and "
        f"{len(truth['incidents'])} incidents to {args.out}"
    )
    return 0


def _ingest(args: argparse.Namespace) -> int:
    from flowlake.ingest import ingest_path

    results = ingest_path(
        Lake(args.lake),
        args.path,
        sensor_id=args.sensor,
        binary=args.flowsentinel_bin,
        force=args.force,
        workers=args.workers,
    )
    _print_results(results)
    return 1 if any(result.status == "failed" for result in results) else 0


def _print_results(results: list[BatchResult]) -> None:
    statuses = Counter(result.status for result in results)
    reasons: Counter[str] = Counter()
    for result in results:
        reasons.update(result.quarantine_reasons)
    written = sum(r.records_written for r in results if r.status == "ingested")
    quarantined = sum(r.records_quarantined for r in results if r.status == "ingested")
    print(
        f"batches: {dict(sorted(statuses.items()))}; records written: {written:,}; "
        f"quarantined: {quarantined:,} {dict(sorted(reasons.items())) if reasons else ''}"
    )
    for result in results:
        if result.status in ("rejected", "failed"):
            print(f"{result.status} {result.input_ref}: {result.error}")


def _transform(args: argparse.Namespace) -> int:
    from flowlake.transform import build, source_freshness

    lake = Lake(args.lake)
    result = build(lake, full_refresh=args.full_refresh, select=args.select, variables=args.vars)
    if not result.success:
        return 1
    if args.freshness and not source_freshness(lake).success:
        return 1
    return 0


def _evaluate(args: argparse.Namespace) -> int:
    from flowlake.evaluate import evaluate, format_evaluation

    evaluation = evaluate(Lake(args.lake), json.loads(args.ground_truth.read_text()))
    print(format_evaluation(evaluation))
    return 0 if evaluation.passed else 1


def _report(args: argparse.Namespace) -> int:
    from flowlake.report import write_report

    truth = json.loads(args.ground_truth.read_text()) if args.ground_truth else None
    path = write_report(Lake(args.lake), args.out, ground_truth=truth)
    print(f"wrote {path}")
    return 0


def _demo(args: argparse.Namespace) -> int:
    from flowlake.evaluate import evaluate, format_evaluation
    from flowlake.ingest import ingest_path
    from flowlake.report import write_report
    from flowlake.sources.synthetic import generate
    from flowlake.transform import build

    lake = Lake(args.lake)
    print(f"1/5 generating synthetic captures in {args.landing}")
    truth = generate(_synthetic_config(args), args.landing)
    print(f"2/5 ingesting into {args.lake}")
    _print_results(ingest_path(lake, args.landing, workers=args.workers))
    print("3/5 running dbt build")
    if not build(lake).success:
        return 1
    print("4/5 scoring detections against the ground truth")
    evaluation = evaluate(lake, truth)
    print(format_evaluation(evaluation))
    print("5/5 writing the dashboard")
    print(f"wrote {write_report(lake, args.out, ground_truth=truth)}")
    return 0 if evaluation.passed else 1


def _contract(args: argparse.Namespace) -> int:
    from flowlake.contract import json_schemas, render_schema

    stale = []
    for name, schema in json_schemas().items():
        target = args.out / name
        text = render_schema(schema)
        if args.check:
            if not target.exists() or target.read_text(encoding="utf-8") != text:
                stale.append(str(target))
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text, encoding="utf-8")
            print(f"wrote {target}")
    if stale:
        print(f"out of date: {', '.join(stale)}; run `flowlake contract`", file=sys.stderr)
        return 1
    return 0


def _produce(args: argparse.Namespace) -> int:
    from flowlake.streaming import ensure_topic, produce

    ensure_topic(args.bootstrap, args.topic)
    captures = _file_captures(args) if args.path else _synthetic_captures(args)
    count = produce(captures, bootstrap=args.bootstrap, topic=args.topic)
    print(f"published {count:,} flows to {args.topic}")
    return 0


def _file_captures(args: argparse.Namespace) -> Iterator[CaptureFlows]:
    from flowlake.ingest import DEFAULT_SENSOR, discover, sensor_from_path
    from flowlake.sources.flowsentinel import (
        SourceError,
        parse_document,
        run_flows_cli,
        sha256_file,
        sha256_hex,
    )
    from flowlake.streaming import CaptureFlows

    for file in discover(args.path):
        sensor = args.sensor or sensor_from_path(file) or DEFAULT_SENSOR
        try:
            if file.suffix.lower() == ".pcap":
                data, digest = run_flows_cli(file, binary=args.flowsentinel_bin), sha256_file(file)
            else:
                data = file.read_bytes()
                digest = sha256_hex(data)
            capture = parse_document(data)
        except SourceError as exc:
            print(f"skipped {file}: {exc}", file=sys.stderr)
            continue
        yield CaptureFlows(
            sensor,
            f"sha256:{digest}",
            capture.capture_file,
            capture.completion_state,
            capture.flows,
        )


def _synthetic_captures(args: argparse.Namespace) -> Iterator[CaptureFlows]:
    from flowlake.sources.flowsentinel import sha256_hex
    from flowlake.sources.synthetic import SyntheticNetwork, serialize
    from flowlake.streaming import CaptureFlows

    for capture in SyntheticNetwork(_synthetic_config(args)).captures():
        document = capture.document
        yield CaptureFlows(
            capture.sensor_id,
            f"sha256:{sha256_hex(serialize(document))}",
            document["capture"]["file_name"],
            document["completion_state"],
            document["flows"],
        )


def _consume(args: argparse.Namespace) -> int:
    from flowlake.streaming import BronzeConsumer, ensure_topic

    ensure_topic(args.bootstrap, args.topic)
    consumer = BronzeConsumer(
        Lake(args.lake),
        bootstrap=args.bootstrap,
        topic=args.topic,
        group_id=args.group,
        batch_size=args.batch_size,
        batch_seconds=args.batch_seconds,
    )
    with contextlib.suppress(KeyboardInterrupt):  # Ctrl-C stops consuming cleanly
        consumer.run(idle_timeout=args.idle_timeout)
    _print_results(consumer.results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
