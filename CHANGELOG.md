# Changelog

All notable changes are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and versions follow
[Semantic Versioning](https://semver.org/).

## [0.2.1] - 2026-10-09

### Fixed

- `flowlake stream consume --idle-timeout` could stop before its consumer group had joined
  (a slow first connection or a broker's rebalance delay) and read nothing. The idle clock now
  starts when partitions are assigned, and a group that is never joined is reported as an
  error after 60 seconds.

### Changed

- GitHub Actions moved to their current versions (`checkout` 7, `setup-uv` 7,
  `upload-artifact` 6, `upload-pages-artifact` 5, `deploy-pages` 5), and the optional Redpanda
  Console to v3.12.0.
- Dependency updates are no longer automated: `make upgrade` moves every Python dependency to
  its newest allowed version and runs the checks.

## [0.2.0] - 2026-10-09

The lakehouse becomes something you can run on your own network: a self-hosted suite, real
capture handling and per-deployment configuration.

### Added

- **The FlowSentinel Suite** in `deploy/`: one Docker Compose stack with FlowSentinel's server
  and web app, PostgreSQL, the Dagster UI and daemon, the lakehouse dashboard and an optional
  Redpanda streaming path. `setup.sh` generates every secret on the machine. Containers run
  unprivileged with read-only file systems and no capabilities, and ports bind to 127.0.0.1.
  An end-to-end smoke test builds and runs the whole stack in CI. See
  [docs/deployment.md](https://github.com/exosphere8/flowsentinel-lakehouse/blob/main/docs/deployment.md).
- A container image for the lakehouse with the FlowSentinel CLI built in.
- **Deployment configuration**: your network zones, an allowlist for known-good traffic and
  detection thresholds, in one directory (`--config` or `FLOWLAKE_CONFIG`). Every file is
  validated before anything runs; `flowlake config` checks it. See
  [docs/configuration.md](https://github.com/exosphere8/flowsentinel-lakehouse/blob/main/docs/configuration.md).
- **pcapng** captures are converted to pcap in pure Python (any byte order, timestamp
  resolution and offset; one capture per interface), and `.cap` files are accepted.
- Captures over a million packets, FlowSentinel's per-run maximum, are split into parts and
  ingested as one source.
- Captures that FlowSentinel cut short at a limit are flagged in `dq_batches`, warned about by
  a data test, and counted on the dashboard. `FLOWLAKE_FLOWSENTINEL_ARGS` sets the limits.
- A `full_refresh` option on the Dagster dbt step, to rebuild the incremental models from
  bronze.
- `FLOWLAKE_AUTOMATION=on` starts the inbox sensor and the hourly schedule enabled.
- When the pipeline daemon starts, it marks runs that a restart interrupted as failed, so they
  cannot hold the one-run queue.
- The dbt project ships inside the wheel, so `pip install` gives a working lakehouse.

### Changed

- The allowlist applies to every detection, not only beaconing, and the default NTP entry
  matches any destination.
- The default network zones are generic (RFC 1918, link-local, loopback). The demo's office
  zones moved into the demo's own configuration.
- The inbox sensor fingerprints file names, sizes and times, so files copied with their
  original timestamps (`rsync -a`, `cp -p`) start a run too.
- `flowlake ingest` exits with status 1 when a capture could not be decoded.

### Fixed

- A file that cannot be read, for example because of its permissions, fails on its own and is
  retried on the next run, instead of stopping the ingestion of every other file.
- A capture that could not be prepared because of an I/O error (such as a full disk) is retried
  instead of being rejected for good.

### Upgrading from 0.1.0

The default zones changed. If you loaded your own data with 0.1.0, add your ranges to
`network_zones.csv` and run one full refresh.

## [0.1.0] - 2026-10-05

First release: data contract and quarantine, batch and streaming ingestion into Parquet, the
dbt medallion model on DuckDB with four SQL detections mapped to MITRE ATT&CK and an OCSF
export, Dagster orchestration, the static dashboard, benchmarks and CI.

[0.2.1]: https://github.com/exosphere8/flowsentinel-lakehouse/releases/tag/v0.2.1
[0.2.0]: https://github.com/exosphere8/flowsentinel-lakehouse/releases/tag/v0.2.0
[0.1.0]: https://github.com/exosphere8/flowsentinel-lakehouse/tree/8e25305
