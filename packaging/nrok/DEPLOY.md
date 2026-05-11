# MobilityNebula deployment runbook for SNCB nroks

Image-only deployment: each nrok pulls a CI-built image from GHCR and fetches
deploy files from GitHub raw. No git clone, no scp, no compilation on the
device. The systemd unit ensures the stack auto-starts on every boot, so a
power-cycled nrok comes back up on its own.

## Prerequisites on each nrok

- Docker Engine: `docker --version` (≥ 24.x)
- Docker Compose v2: `docker compose version` (≥ 2.20)
- Outbound HTTPS to `ghcr.io` and `raw.githubusercontent.com`
- Root/sudo access
- ~2 GB free disk

If Docker is missing:

```bash
sudo apt-get update && sudo apt-get install -y docker.io docker-compose-plugin
sudo systemctl enable --now docker
```

## One-time HQ checks (per release)

1. GHCR package `marianagarcez/mobility-nebula` is **Public**.
   https://github.com/marianaGarcez?tab=packages → `mobility-nebula` →
   Package settings → Visibility = Public

2. The CI workflow `.github/workflows/nrok_release.yml` is green for the
   commit you want to ship. Check
   https://github.com/marianaGarcez/MobilityNebula/actions.

3. Choose the **deployment ref**:
   - Testing: branch name (`feature/upstream-update-tcp-reconnect`)
   - Production: pin to an immutable tag (`v1.0.0`, `sha-abc1234`)

   Use the same `<REF>` everywhere below.

## Deploy to a fresh nrok (one command)

```bash
ssh <user>@<nrok>
REF="feature/upstream-update-tcp-reconnect"   # or main, or vX.Y.Z
curl -fsSL "https://raw.githubusercontent.com/marianaGarcez/MobilityNebula/${REF}/packaging/nrok/bootstrap.sh" \
    | sudo bash -s -- "${REF}"
```

What the bootstrap does:

1. Pulls `ghcr.io/marianagarcez/mobility-nebula:<REF>` from GHCR.
2. Fetches `docker-compose.runtime.yaml`,
   `packaging/nrok/mobility-nebula-compose.service`, and
   `packaging/nrok/compose.env.example` from GitHub raw at `<REF>`.
3. Extracts query yamls baked into the image at `/opt/mobility-nebula/Queries`.
4. Installs everything to `/opt/mobility-nebula/` (compose + queries) and
   `/etc/mobility-nebula/compose.env` (per-nrok config).
5. Enables `docker.service` and `mobility-nebula-compose.service`. The unit
   runs `docker compose up -d` at boot, so the stack returns automatically
   after a power-cycle.

## Per-nrok configuration

Edit `/etc/mobility-nebula/compose.env`:

```bash
sudo vi /etc/mobility-nebula/compose.env
```

Three knobs:

```
NES_RUNTIME_IMAGE=ghcr.io/marianagarcez/mobility-nebula:<REF>   # image tag
NES_WORKER_THREADS=2                                            # CPU-tune
NES_QUERY_FILE=sncb_brake_monitoring.yaml                       # query to register
```

Apply changes:

```bash
sudo systemctl restart mobility-nebula-compose
```

## Verify

```bash
systemctl is-active mobility-nebula-compose                # → active
docker ps                                                  # → 2 containers up
docker logs nes-worker --tail 20                           # → "Server listening on [::]:8080"
docker logs mobility-nebula-query-registration-1 --tail 20 # → "Query submitted from ..."
```

## Reboot test (validates auto-restart story)

```bash
sudo reboot
# wait ~60s, reconnect:
ssh <user>@<nrok> "systemctl is-active mobility-nebula-compose && docker ps"
```

Both containers should be back up automatically.

## Pushing updates from HQ later

**Push a new query to one train (no image rebuild):**

```bash
ssh <user>@<nrok>
sudo curl -fsSL "https://raw.githubusercontent.com/marianaGarcez/MobilityNebula/${REF}/Queries/<new>.yaml" \
    -o /opt/mobility-nebula/Queries/<new>.yaml
sudo sed -i 's|^NES_QUERY_FILE=.*|NES_QUERY_FILE=<new>.yaml|' /etc/mobility-nebula/compose.env
sudo systemctl restart mobility-nebula-compose
```

**Roll out a new image:**

```bash
ssh <user>@<nrok>
sudo sed -i "s|^NES_RUNTIME_IMAGE=.*|NES_RUNTIME_IMAGE=ghcr.io/marianagarcez/mobility-nebula:<new-tag>|" /etc/mobility-nebula/compose.env
sudo systemctl restart mobility-nebula-compose   # docker compose up -d pulls automatically
```

**Re-run full bootstrap** (picks up updated bootstrap.sh, compose, systemd
unit, queries — and re-pulls the image):

```bash
ssh <user>@<nrok>
curl -fsSL "https://raw.githubusercontent.com/marianaGarcez/MobilityNebula/${REF}/packaging/nrok/bootstrap.sh" \
    | sudo bash -s -- "${REF}"
```

## Fleet rollout

```bash
NROKS=(train01 train02 train42)
REF="v1.0.0"
for host in "${NROKS[@]}"; do
  ssh "$host" "curl -fsSL https://raw.githubusercontent.com/marianaGarcez/MobilityNebula/${REF}/packaging/nrok/bootstrap.sh \
              | sudo bash -s -- ${REF}" &
done
wait
```

## Troubleshooting

| Symptom | Diagnosis | Fix |
|---|---|---|
| systemd unit fails repeatedly | `sudo journalctl -u mobility-nebula-compose.service -n 50` | Read the actual error |
| `Container name "/nes-worker" is already in use` | Stale container from older deploy | Auto-handled by `ExecStartPre=-docker rm -f` in the unit. Manual: `sudo docker rm -f nes-worker query-registration` then restart unit |
| `denied: denied` on `docker pull` | GHCR package not public, or wrong tag | Make package public, OR `docker login ghcr.io -u <user> -p <PAT>` on the nrok |
| `Start request repeated too quickly` | systemd hit the 10-burst restart limit | `sudo systemctl reset-failed mobility-nebula-compose.service && sudo systemctl start mobility-nebula-compose` |
| `nes-cli` yaml errors in registration logs | Query yaml schema invalid | Verify plural `sinks:`, each with `schema:`, correct sink config keys |
| Worker runs but no output | Source can't reach upstream feeder | Verify `socket_host`/`socket_port` reachable from inside the container |

## Uninstall

```bash
sudo systemctl disable --now mobility-nebula-compose.service
sudo docker compose -f /opt/mobility-nebula/docker-compose.runtime.yaml down -v
sudo docker rm -f nes-worker mobility-nebula-query-registration-1 2>/dev/null
sudo rm -rf /opt/mobility-nebula /etc/mobility-nebula
sudo rm /etc/systemd/system/mobility-nebula-compose.service
sudo systemctl daemon-reload
```

## Files installed on the nrok

| Path | Purpose |
|---|---|
| `/opt/mobility-nebula/docker-compose.runtime.yaml` | Compose stack definition |
| `/opt/mobility-nebula/Queries/*.yaml` | Available query definitions |
| `/opt/mobility-nebula/Output/` | File-sink output dir (rw, mounted into worker) |
| `/opt/mobility-nebula/.env` → `/etc/mobility-nebula/compose.env` | Env vars for compose (symlink) |
| `/etc/mobility-nebula/compose.env` | Per-nrok config (image, threads, query) |
| `/etc/mobility-nebula/compose.env.example` | Reference template |
| `/etc/systemd/system/mobility-nebula-compose.service` | systemd unit (auto-start on boot) |

## Architecture summary

```
HQ (developer / CI)
    │ git push  → fork
    │ CI builds → ghcr.io/marianagarcez/mobility-nebula:<tag>
    ▼
nrok (on board the train)
    │ curl bootstrap.sh   (GitHub raw)
    │ docker pull          (GHCR)
    │ systemctl enable     (auto-start on every boot)
    ▼
nes-worker (Up :8080, healthy)  ←→  query-registration (registers query, tails)
```

The nrok never clones the repo, never compiles, never receives files via scp.
Updates are either a fresh `curl bootstrap.sh` or a `sed` on `compose.env`
followed by `systemctl restart`.
