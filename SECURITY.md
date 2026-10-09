# Security Policy

The lakehouse stores network metadata: addresses, names, ports and timings that describe a
network in detail. Treat a deployment, its backups and its dashboard as sensitive.

## Authorized use

Only ingest traffic from networks you own or are explicitly authorized to inspect. The
detections are passive analytics on flow records; nothing in this project probes, blocks or
reaches out to the hosts it sees.

## Reporting a vulnerability

Do **not** open a public issue for security problems.

Report them privately through GitHub's
[private vulnerability reporting](https://github.com/exosphere8/flowsentinel-lakehouse/security/advisories/new).
Include the version or commit, the steps to reproduce and the impact. Use synthetic data only;
never attach real captures or flow records.

You can expect an acknowledgement within 7 days. Once a fix is out, the advisory is published
and credits you unless you prefer otherwise. Problems in FlowSentinel itself go to
[its repository](https://github.com/exosphere8/flowsentinel/security/advisories/new).

## Supported versions

| Version | Supported |
| --- | --- |
| Latest release (0.2.x) | Yes |
| `main` | Yes |
| Older releases | No; upgrade to the latest release |

## How the project protects your data

- **Untrusted input.** Every record passes a strict data contract before it reaches the lake;
  failures are quarantined, not executed or interpreted. Capture files are decoded by
  FlowSentinel's CLI under packet, flow, size and time limits, and the pcapng converter checks
  every block length before reading it.
- **No secrets in the repository.** The suite's database and admin passwords are generated on
  the machine by `deploy/setup.sh`, kept in `deploy/.env` (mode 600) and `deploy/secrets/`
  (mode 700), and git-ignored. Nothing in the images contains a credential.
- **Containers.** Every service runs as an unprivileged user with a read-only root file system,
  all Linux capabilities dropped (PostgreSQL keeps the five its entrypoint needs),
  `no-new-privileges` and a process limit. The pipeline mounts the inbox and the configuration
  read-only.
- **Network exposure.** Every port binds to `127.0.0.1`. The Dagster UI and the dashboard have
  no sign-in of their own, so reach them through an SSH tunnel or an authenticating reverse
  proxy; [docs/deployment.md](docs/deployment.md#security) explains how.
- **Configuration.** Zones, allowlist and thresholds are validated before any run, so a typo
  cannot silently disable a detection. Every allowlist row needs a written reason.
- **Supply chain.** Python dependencies are pinned in `uv.lock` and installed with `--frozen`;
  the FlowSentinel source is pinned to a commit. Updates are deliberate (`make upgrade`), and
  CI runs the full test suite, the contract test against FlowSentinel and the suite smoke test
  on every change.
- **Synthetic test data.** Generated traffic uses documentation and private address ranges and
  reserved domain names, so tests never point at a real host.
