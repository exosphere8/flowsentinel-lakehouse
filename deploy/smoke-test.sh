#!/usr/bin/env bash
# End-to-end test of the self-hosted suite: start it, drop captures into the inbox, and wait
# for the pipeline to turn them into a dashboard with the expected alerts.
#
#   deploy/smoke-test.sh            builds the images (CI)
#   SMOKE_NO_BUILD=1 deploy/smoke-test.sh   uses images that already exist
#
# Uses its own project name, volumes, ports, inbox and configuration, so it never touches a
# suite running on the same machine, and removes all of it at the end.
set -euo pipefail
cd "$(dirname "$0")"

export COMPOSE_PROJECT_NAME="flowsentinel-smoke"
export FLOWSENTINEL_PORT=18080 DASHBOARD_PORT=18088 DAGSTER_PORT=13000
work="$(mktemp -d)"
chmod 755 "$work"  # the containers' own users read the inbox
export INBOX_DIR="$work/inbox" CONFIG_DIR="$work/config"
mkdir -p "$INBOX_DIR" "$CONFIG_DIR"
TIMEOUT_SECONDS="${SMOKE_TIMEOUT_SECONDS:-900}"
fail() { echo "SMOKE TEST FAILED: $*" >&2; docker compose logs --tail 150 >&2 || true; exit 1; }
cleanup() { docker compose down -v --remove-orphans >/dev/null 2>&1 || true; rm -rf "$work"; }
trap cleanup EXIT

./setup.sh >/dev/null
password="$(tr -d '\n' < secrets/flowsentinel_admin_password)"

echo "== starting the suite"
if [ -n "${SMOKE_NO_BUILD:-}" ]; then
    docker compose up -d --no-build --wait --wait-timeout 600 || fail "services did not become healthy"
else
    docker compose up -d --build --wait --wait-timeout 1800 || fail "services did not become healthy"
fi

echo "== FlowSentinel answers and the generated admin can sign in"
curl -fsS http://127.0.0.1:18080/health | grep -q '"status":"ok"' || fail "FlowSentinel /health"
status="$(curl -s -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' \
    -d "{\"username\":\"admin\",\"password\":\"$password\"}" http://127.0.0.1:18080/api/v1/auth/login)"
[ "$status" = "200" ] || fail "admin sign-in returned HTTP $status"

echo "== dropping captures into the inbox"
# A real FlowSentinel capture as pcap and as pcapng, and a day of synthetic office traffic.
mkdir -p "$INBOX_DIR/sensor=smoke-lab" "$INBOX_DIR/sensor=smoke-lab-ng" "$INBOX_DIR/sensor=smoke-synthetic"
cp ../tests/fixtures/pcap/flows-mixed.pcap "$INBOX_DIR/sensor=smoke-lab/"
cp ../tests/fixtures/pcap/flows-mixed.pcapng "$INBOX_DIR/sensor=smoke-lab-ng/"
docker compose run --rm --no-deps -T --user "$(id -u):$(id -g)" \
    -v "$INBOX_DIR/sensor=smoke-synthetic:/out" \
    --entrypoint flowlake lakehouse-ui generate --out /out --days 1 --workstations 6 \
    || fail "could not generate synthetic captures"

echo "== waiting for the pipeline (up to ${TIMEOUT_SECONDS}s)"
deadline=$((SECONDS + TIMEOUT_SECONDS))
until curl -fsS http://127.0.0.1:18088/ 2>/dev/null | grep -q 'class="sev sev-'; do
    [ $SECONDS -lt $deadline ] || fail "the dashboard showed no alerts within ${TIMEOUT_SECONDS}s"
    sleep 15
done
# Let the run that produced the dashboard finish before reading its results.
sleep 20

echo "== checking the results"
page="$(curl -fsS http://127.0.0.1:18088/)"
alerts="$(grep -o 'class="sev sev-' <<<"$page" | wc -l)"
[ "$alerts" -eq 4 ] || fail "expected 4 alerts on the dashboard, found $alerts"
for rule in "Large outbound transfer" "Periodic beaconing" "DNS tunneling" "Port scan"; do
    grep -q "$rule" <<<"$page" || fail "dashboard is missing the $rule alert"
done
docker compose exec -T lakehouse-daemon python - <<'PY' || fail "the captures were not ingested through the FlowSentinel CLI"
import json
from pathlib import Path

entries = [json.loads(p.read_text()) for p in Path("/data/lake/_ledger").glob("*.json")]
def source(name):
    found = [e for e in entries if e["input_ref"].endswith(name) and e["source"] == "flowsentinel_pcap"]
    assert found, f"no ledger entry for {name}"
    return found[0]

pcap, pcapng = source("smoke-lab/flows-mixed.pcap"), source("smoke-lab-ng/flows-mixed.pcapng")
assert pcapng["converted"], pcapng
assert pcap["records_written"] == pcapng["records_written"] > 0, (pcap, pcapng)
print(f"pcap and pcapng each gave {pcap['records_written']} flows")
PY
runs="$(curl -fsS -H 'Content-Type: application/json' http://127.0.0.1:13000/graphql \
    -d '{"query":"{ runsOrError(filter: {statuses: [SUCCESS]}) { ... on Runs { results { runId } } } }"}')"
grep -q '"runId"' <<<"$runs" || fail "Dagster has no successful run"

echo "SMOKE TEST PASSED: $alerts alerts on the dashboard, pcap and pcapng ingested, Dagster runs succeeded"
