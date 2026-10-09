#!/bin/sh
# Prepares /data on first start, validates the configuration, then runs the command.
set -eu

mkdir -p "$FLOWLAKE_LAKE" "$FLOWLAKE_SITE" "$DAGSTER_HOME" "$TMPDIR"
for dir in "$FLOWLAKE_LAKE" "$FLOWLAKE_SITE" "$DAGSTER_HOME" "$TMPDIR"; do
    if [ ! -w "$dir" ]; then
        echo "error: $dir is not writable by uid $(id -u). Its volume was probably created by" \
            "another container; see docs/deployment.md#troubleshooting" >&2
        exit 1
    fi
done
# Scratch space for capture conversion lives on the data volume, not in memory. Leftovers
# from a crash are removed after a day; newer ones may belong to another running container.
find "$TMPDIR" -mindepth 1 -maxdepth 1 -mmin +1440 -exec rm -rf {} + 2>/dev/null || true
[ -f "$DAGSTER_HOME/dagster.yaml" ] || cp /opt/flowlake/dagster.yaml "$DAGSTER_HOME/dagster.yaml"

if [ ! -f "$FLOWLAKE_SITE/index.html" ]; then
    cat > "$FLOWLAKE_SITE/index.html" <<'HTML'
<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>FlowSentinel Lakehouse</title>
<style>body{font:16px/1.6 system-ui,sans-serif;max-width:40rem;margin:4rem auto;padding:0 1rem;color:#222;background:#fafaf8}
code{background:#eee;padding:.1rem .3rem;border-radius:4px}@media(prefers-color-scheme:dark){body{background:#111;color:#eee}code{background:#333}}</style>
</head><body><h1>Waiting for data</h1>
<p>The dashboard appears after the first pipeline run. Put capture files (<code>.pcap</code>,
<code>.pcapng</code>) or FlowSentinel JSON into <code>inbox/sensor=&lt;name&gt;/</code> and the
pipeline picks them up within a minute. Progress is visible in Dagster.</p></body></html>
HTML
fi

# Fail fast, with a readable message, on an invalid configuration.
if [ -n "${FLOWLAKE_CONFIG:-}" ]; then
    flowlake config >/dev/null
fi

# Runs execute inside the daemon, so when it starts none can still be running. Fail the ones a
# restart interrupted; otherwise they hold the one-run queue forever.
if [ "${1:-}" = "dagster-daemon" ]; then
    python - <<'PY'
from flowlake.orchestration import fail_interrupted_runs

for run_id in fail_interrupted_runs():
    print(f"run {run_id} was interrupted when the daemon stopped; marked it as failed")
PY
fi

exec "$@"
