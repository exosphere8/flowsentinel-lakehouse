#!/bin/sh
# Prints the release notes for one version: its CHANGELOG.md section and how to install it.
#
#   scripts/release-notes.sh 0.2.0
set -eu
version="$1"
cd "$(dirname "$0")/.."

notes="$(awk -v heading="## [$version]" '
    index($0, heading) == 1 { found = 1; next }
    found && /^## \[/ { exit }
    found { print }
' CHANGELOG.md)"
if [ -z "$(printf '%s' "$notes" | tr -d '[:space:]')" ]; then
    echo "CHANGELOG.md has no section for $version" >&2
    exit 1
fi

printf '%s\n' "$notes" | sed -e '/./,$!d'
cat <<NOTES

### Install

**The self-hosted suite** (FlowSentinel, the pipeline, Dagster and the dashboard): download the
source code archive below, then follow the
[deployment guide](https://github.com/exosphere8/flowsentinel-lakehouse/blob/v$version/docs/deployment.md).

**The Python package** (pipeline and \`flowlake\` command): \`pip install\` the wheel below.
NOTES
