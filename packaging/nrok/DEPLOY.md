# Deploying MobilityNebula to an nrok

## 1. Prerequisites on the nrok

- Docker installed (`docker --version`)
- Outbound internet to `ghcr.io` and `raw.githubusercontent.com`
- Root/sudo access

If Docker is missing:

```bash
sudo apt-get update && sudo apt-get install -y docker.io docker-compose-plugin
```

## 2. Deploy (one command)

Pick a ref. For SNCB production today, use the feature branch:

```bash
REF="feature/upstream-update-tcp-reconnect"
```

Then on the nrok:

```bash
curl -fsSL "https://raw.githubusercontent.com/marianaGarcez/MobilityNebula/${REF}/packaging/nrok/bootstrap.sh" \
    | sudo bash -s -- "${REF}"
```

That's it. The script pulls the image, drops the config files into place, and enables the auto-restart systemd unit.

## 3. Configure which query runs

```bash
sudo vi /etc/mobility-nebula/compose.env
```

Change `NES_QUERY_FILE=` to the yaml you want. Then:

```bash
sudo systemctl restart mobility-nebula-compose
```

## 4. Verify

```bash
systemctl is-active mobility-nebula-compose    # active
docker ps                                       # nes-worker + query-registration both up
```

## 5. Confirm reboot survival (one-time check per nrok)

```bash
sudo reboot
# wait ~60s, reconnect:
systemctl is-active mobility-nebula-compose && docker ps
```

Both containers should be back up. If yes, you're done.

---

## Updating later

**New query:** replace the yaml in `/opt/mobility-nebula/Queries/`, point `NES_QUERY_FILE` at it, restart.

**New image:** change `NES_RUNTIME_IMAGE` in `/etc/mobility-nebula/compose.env`, restart.

**Pick up everything fresh (image + scripts + queries):** rerun the same `curl … bootstrap.sh` command from step 2.

## Fleet rollout

```bash
REF="v1.0.0"
for host in train01 train02 train42; do
  ssh "$host" "curl -fsSL https://raw.githubusercontent.com/marianaGarcez/MobilityNebula/${REF}/packaging/nrok/bootstrap.sh \
              | sudo bash -s -- ${REF}" &
done
wait
```

## If something's wrong

```bash
sudo journalctl -u mobility-nebula-compose.service -n 50
docker logs nes-worker --tail 50
docker logs mobility-nebula-query-registration-1 --tail 50
```

Common fixes:

- **`denied: denied` on pull** — GHCR package not public; visit
  https://github.com/marianaGarcez?tab=packages → `mobility-nebula` → set Public.
- **`Container name "/nes-worker" already in use`** — should be auto-cleaned;
  manual: `sudo docker rm -f nes-worker query-registration && sudo systemctl restart mobility-nebula-compose`.
- **`Start request repeated too quickly`** — systemd gave up retrying:
  `sudo systemctl reset-failed mobility-nebula-compose.service && sudo systemctl start mobility-nebula-compose`.

## Uninstall

```bash
sudo systemctl disable --now mobility-nebula-compose.service
sudo docker compose -f /opt/mobility-nebula/docker-compose.runtime.yaml down -v
sudo rm -rf /opt/mobility-nebula /etc/mobility-nebula
sudo rm /etc/systemd/system/mobility-nebula-compose.service
sudo systemctl daemon-reload
```
