#!/bin/sh
# One-time setup for the FlowSentinel Suite. Safe to run again: it never overwrites secrets.
#
#   ./setup.sh
#
# Creates:
#   .env                                  settings, with a random PostgreSQL password
#   secrets/flowsentinel_admin_password   the first FlowSentinel admin's password
#   inbox/sensor=default/                 where capture files go
#   config/                               your network zones, allowlist, thresholds (optional)
set -eu
cd "$(dirname "$0")"

random_secret() {
    # 32 letters and digits: no URL encoding needed in the database URL.
    LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 32
}

umask 077
if [ ! -f .env ]; then
    sed "s/^POSTGRES_PASSWORD=.*/POSTGRES_PASSWORD=$(random_secret)/" .env.example > .env
    echo "created .env with a random database password"
else
    echo ".env exists, left as is"
fi

mkdir -p secrets
chmod 700 secrets
if [ ! -s secrets/flowsentinel_admin_password ]; then
    password="$(random_secret)"
    printf '%s\n' "$password" > secrets/flowsentinel_admin_password
    # The directory is private; the file must be readable by the container's own user.
    chmod 644 secrets/flowsentinel_admin_password
    echo "created the FlowSentinel admin password (user: admin):"
    echo "    $password"
    echo "it is stored in deploy/secrets/flowsentinel_admin_password"
else
    echo "admin password exists, left as is"
fi

umask 022
mkdir -p "inbox/sensor=default" config
echo
echo "next: docker compose up -d --build"
echo "then put .pcap/.pcapng files into inbox/sensor=default/ (or inbox/sensor=<name>/)"
