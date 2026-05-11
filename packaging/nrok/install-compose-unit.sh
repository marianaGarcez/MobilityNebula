#!/usr/bin/env bash
# Install the MobilityNebula compose stack on an SNCB on-board nrok device.
#
# Two layouts supported, auto-detected from the script's location:
#   * Bundle mode  — all sources sit next to this script (tarball deploy).
#   * Repo mode    — script lives in packaging/nrok/ of a git checkout;
#                    compose file and Queries/ are taken from the repo root.
#
# Usage (as root):
#   sudo ./install-compose-unit.sh
#
# Idempotent: re-running picks up updated files. The per-device compose.env is
# preserved if it already exists.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
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

UNIT_SRC="$SCRIPT_DIR/mobility-nebula-compose.service"
ENV_SRC="$SCRIPT_DIR/compose.env.example"

if [[ -f "$SCRIPT_DIR/docker-compose.runtime.yaml" && -d "$SCRIPT_DIR/Queries" ]]; then
    MODE="bundle"
    COMPOSE_SRC="$SCRIPT_DIR/docker-compose.runtime.yaml"
    QUERIES_SRC="$SCRIPT_DIR/Queries"
elif [[ -f "$SCRIPT_DIR/../../docker-compose.runtime.yaml" && -d "$SCRIPT_DIR/../../Queries" ]]; then
    MODE="repo"
    REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
    COMPOSE_SRC="$REPO_ROOT/docker-compose.runtime.yaml"
    QUERIES_SRC="$REPO_ROOT/Queries"
else
    echo "Cannot locate docker-compose.runtime.yaml + Queries/ next to this script or at \$SCRIPT_DIR/../.." >&2
    exit 1
fi

for f in "$UNIT_SRC" "$ENV_SRC"; do
    if [[ ! -f "$f" ]]; then
        echo "Missing $f (must live next to install-compose-unit.sh)" >&2
        exit 1
    fi
done

echo "Install mode: $MODE"
echo "  compose:  $COMPOSE_SRC"
echo "  queries:  $QUERIES_SRC"

systemctl enable docker >/dev/null

install -d "$INSTALL_DIR" "$INSTALL_DIR/Queries" "$INSTALL_DIR/Output" "$ENV_DIR"

install -m 0644 "$COMPOSE_SRC" "$INSTALL_DIR/docker-compose.runtime.yaml"

# Copy queries into the install dir; missing-from-source queries on the device
# are left alone so HQ can layer per-train hot-fixes without losing them.
for query in "$QUERIES_SRC"/*.yaml; do
    [[ -f "$query" ]] || continue
    install -m 0644 "$query" "$INSTALL_DIR/Queries/$(basename "$query")"
done

install -m 0644 "$ENV_SRC" "$ENV_DIR/compose.env.example"
if [[ ! -f "$ENV_DIR/compose.env" ]]; then
    install -m 0644 "$ENV_SRC" "$ENV_DIR/compose.env"
    echo "Created $ENV_DIR/compose.env from template; edit it to set NES_QUERY_FILE and image tag for this train."
fi

# Make the env file readable by docker-compose at the install dir too. systemd
# uses /etc/mobility-nebula/compose.env via EnvironmentFile; compose itself
# auto-loads .env from the working directory.
ln -sfn "$ENV_DIR/compose.env" "$INSTALL_DIR/.env"

install -m 0644 "$UNIT_SRC" "$UNIT_DST"
systemctl daemon-reload
systemctl enable --now mobility-nebula-compose.service

systemctl --no-pager status mobility-nebula-compose.service || true
