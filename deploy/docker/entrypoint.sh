#!/bin/sh
# Prepares /data on first start, validates the configuration, then runs the command.
set -eu

mkdir -p "$FLOWLAKE_LAKE" "$FLOWLAKE_SITE" "$DAGSTER_HOME"
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

exec "$@"
