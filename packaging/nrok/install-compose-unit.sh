#!/usr/bin/env bash
# Install the MobilityNebula compose stack on an SNCB on-board nrok device.
#
# This installer is bundle-based: it expects to be unpacked alongside the
# compose file, systemd unit, env template, and Queries/ directory. No git
# checkout or source tree is required on the device.
#
# Expected layout (anywhere on the device):
#   <bundle>/
#       install.sh                          (this script)
#       docker-compose.runtime.yaml
#       mobility-nebula-compose.service
#       compose.env.example
#       Queries/
#           <one or more *.yaml>
#
# Usage (as root):
#   sudo ./install.sh
#
# Idempotent: re-running picks up updated files in the bundle. The per-device
# compose.env is preserved if it already exists.
set -euo pipefail

BUNDLE_DIR="$(cd "$(dirname "$0")" && pwd)"
INSTALL_DIR="/opt/mobility-nebula"
ENV_DIR="/etc/mobility-nebula"
UNIT_DST="/etc/systemd/system/mobility-nebula-compose.service"

if [[ $EUID -ne 0 ]]; then
    echo "Must be run as root (use sudo)." >&2
    exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "docker is not installed; install Docker Engine before running this script." >&2
    exit 1
fi

for required in docker-compose.runtime.yaml mobility-nebula-compose.service compose.env.example; do
    if [[ ! -f "$BUNDLE_DIR/$required" ]]; then
        echo "Missing $required in bundle directory $BUNDLE_DIR" >&2
        exit 1
    fi
done

if [[ ! -d "$BUNDLE_DIR/Queries" ]]; then
    echo "Missing Queries/ directory in bundle $BUNDLE_DIR" >&2
    exit 1
fi

systemctl enable docker >/dev/null

install -d "$INSTALL_DIR" "$INSTALL_DIR/Queries" "$INSTALL_DIR/Output" "$ENV_DIR"

install -m 0644 "$BUNDLE_DIR/docker-compose.runtime.yaml" "$INSTALL_DIR/docker-compose.runtime.yaml"

# Sync queries: copy bundle queries into the install dir. Existing host queries
# not present in the bundle are left alone so HQ can layer hot-fixes per train.
for query in "$BUNDLE_DIR/Queries/"*.yaml; do
    [[ -f "$query" ]] || continue
    install -m 0644 "$query" "$INSTALL_DIR/Queries/$(basename "$query")"
done

install -m 0644 "$BUNDLE_DIR/compose.env.example" "$ENV_DIR/compose.env.example"
if [[ ! -f "$ENV_DIR/compose.env" ]]; then
    install -m 0644 "$BUNDLE_DIR/compose.env.example" "$ENV_DIR/compose.env"
    echo "Created $ENV_DIR/compose.env from template; edit it to set NES_QUERY_FILE and image tag for this train."
fi

# Make the env file readable by docker-compose at the install dir too. systemd
# uses /etc/mobility-nebula/compose.env via EnvironmentFile; compose itself
# auto-loads .env from the working directory.
ln -sfn "$ENV_DIR/compose.env" "$INSTALL_DIR/.env"

install -m 0644 "$BUNDLE_DIR/mobility-nebula-compose.service" "$UNIT_DST"
systemctl daemon-reload
systemctl enable --now mobility-nebula-compose.service

systemctl --no-pager status mobility-nebula-compose.service || true
