#!/usr/bin/env bash
# Assemble a self-contained deploy bundle for SNCB on-board nroks.
#
# Run this from anywhere in the repo. The output is a single tarball that HQ
# scps to a train. The tarball contains everything the device needs to come
# up with Docker only — no git clone required on the nrok.
#
# Usage:
#   packaging/nrok/make-deploy-bundle.sh [query1.yaml query2.yaml ...]
#
# If query files are omitted, every yaml in Queries/ is included.
#
# Output: ./mobility-nebula-deploy-<timestamp>.tar.gz
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
STAGE="$(mktemp -d -t mobility-nebula-deploy.XXXXXX)"
trap 'rm -rf "$STAGE"' EXIT

BUNDLE_NAME="mobility-nebula-deploy-$(date -u +%Y%m%dT%H%M%SZ)"
BUNDLE_DIR="$STAGE/$BUNDLE_NAME"
install -d "$BUNDLE_DIR/Queries"

install -m 0644 "$REPO_ROOT/docker-compose.runtime.yaml"          "$BUNDLE_DIR/docker-compose.runtime.yaml"
install -m 0644 "$SCRIPT_DIR/mobility-nebula-compose.service"     "$BUNDLE_DIR/mobility-nebula-compose.service"
install -m 0644 "$SCRIPT_DIR/compose.env.example"                 "$BUNDLE_DIR/compose.env.example"
install -m 0755 "$SCRIPT_DIR/install-compose-unit.sh"             "$BUNDLE_DIR/install.sh"

if [[ $# -gt 0 ]]; then
    for q in "$@"; do
        src="$REPO_ROOT/Queries/$q"
        if [[ ! -f "$src" ]]; then
            echo "Query not found: $src" >&2
            exit 1
        fi
        install -m 0644 "$src" "$BUNDLE_DIR/Queries/$q"
    done
else
    for q in "$REPO_ROOT/Queries/"*.yaml; do
        [[ -f "$q" ]] || continue
        install -m 0644 "$q" "$BUNDLE_DIR/Queries/$(basename "$q")"
    done
fi

OUT="$REPO_ROOT/$BUNDLE_NAME.tar.gz"
tar -czf "$OUT" -C "$STAGE" "$BUNDLE_NAME"

echo "Bundle ready: $OUT"
echo
echo "Deploy to a train:"
echo "  scp $OUT nrok-train-N:/tmp/"
echo "  ssh nrok-train-N \"sudo tar -xzf /tmp/$(basename "$OUT") -C /tmp && sudo /tmp/$BUNDLE_NAME/install.sh\""
