#!/usr/bin/env bash
# Image-only bootstrap for a fresh nrok / AWS box.
#
# Pulls the CI-built MobilityNebula image, extracts the deploy files baked
# inside it (compose file, systemd unit, env example, query yamls), and
# installs them so the stack auto-starts on every boot.
#
# No git checkout and no source code is left on the device — only the image,
# the systemd unit, the compose file, and the per-device env.
#
# Usage on the nrok (one line, as root):
#   curl -fsSL https://raw.githubusercontent.com/marianaGarcez/MobilityNebula/<branch>/packaging/nrok/bootstrap.sh \
#       | sudo bash -s -- <branch-or-tag>
#
# Example for the SNCB feature branch:
#   curl -fsSL https://raw.githubusercontent.com/marianaGarcez/MobilityNebula/feature/upstream-update-tcp-reconnect/packaging/nrok/bootstrap.sh \
#       | sudo bash -s -- feature/upstream-update-tcp-reconnect
#
# Prerequisites:
#   - docker installed and the service enabled (the script enables it again to be safe)
#   - the ghcr.io/marianagarcez/mobility-nebula package is Public, or
#     `docker login ghcr.io` was performed before running this script.
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "Run as root (sudo)." >&2
    exit 1
fi

REF="${1:-main}"
# CI (docker/metadata-action type=ref,event=branch) replaces "/" with "-"
TAG="${REF//\//-}"
REGISTRY="ghcr.io/marianagarcez/mobility-nebula"
IMAGE="$REGISTRY:$TAG"

INSTALL_DIR="/opt/mobility-nebula"
ENV_DIR="/etc/mobility-nebula"
UNIT_DST="/etc/systemd/system/mobility-nebula-compose.service"

command -v docker >/dev/null || { echo "docker is required" >&2; exit 1; }

echo "==> Pulling $IMAGE"
docker pull "$IMAGE"

echo "==> Extracting deployment files from image"
CID="$(docker create "$IMAGE" true)"
trap 'docker rm -f "$CID" >/dev/null 2>&1 || true' EXIT

install -d "$INSTALL_DIR" "$INSTALL_DIR/Queries" "$INSTALL_DIR/Output" "$ENV_DIR"

docker cp "$CID:/workspace/docker-compose.runtime.yaml"               "$INSTALL_DIR/docker-compose.runtime.yaml"
docker cp "$CID:/workspace/packaging/nrok/mobility-nebula-compose.service" "$UNIT_DST"
docker cp "$CID:/workspace/packaging/nrok/compose.env.example"        "$ENV_DIR/compose.env.example"

# Copy every baked-in query yaml. Existing host queries not in the image are
# left in place so HQ can layer per-train overrides without losing them.
TMPQ="$(mktemp -d)"
docker cp "$CID:/workspace/Queries/." "$TMPQ/"
for q in "$TMPQ"/*.yaml; do
    [[ -f "$q" ]] || continue
    install -m 0644 "$q" "$INSTALL_DIR/Queries/$(basename "$q")"
done
rm -rf "$TMPQ"

if [[ ! -f "$ENV_DIR/compose.env" ]]; then
    install -m 0644 "$ENV_DIR/compose.env.example" "$ENV_DIR/compose.env"
    # Pin the image tag to whatever this bootstrap pulled, so reboots use the
    # same image even if the branch tag later moves.
    sed -i "s|^NES_RUNTIME_IMAGE=.*|NES_RUNTIME_IMAGE=$IMAGE|" "$ENV_DIR/compose.env"
    echo "Created $ENV_DIR/compose.env (image pinned to $IMAGE)."
fi

ln -sfn "$ENV_DIR/compose.env" "$INSTALL_DIR/.env"

systemctl enable docker >/dev/null
systemctl daemon-reload
systemctl enable --now mobility-nebula-compose.service

systemctl --no-pager status mobility-nebula-compose.service || true
