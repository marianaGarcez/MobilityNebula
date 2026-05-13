# MobilityNebula — install on a train

## What you need

- The nrok has internet access and docker installed

## 1. Run the installer (copy-paste)

```bash
curl -fsSL "https://raw.githubusercontent.com/marianaGarcez/MobilityNebula/feature/upstream-update-tcp-reconnect/packaging/nrok/bootstrap.sh" \
    | sudo bash -s -- feature/upstream-update-tcp-reconnect
```

You will see the line
`Finished mobility-nebula-compose.service` when it's done.

## 2. Check it's running

```bash
docker ps
```

You should see three containers running:
- `mosquitto`
- `nes-worker`
- `mobility-nebula-query-registration-1`

The system should auto-start every time the nrok reboots

## To change which query runs

Edit `/etc/mobility-nebula/compose.env` and change the `NES_QUERY_FILE=`
line to the yaml you want. Then:

```bash
sudo systemctl restart mobility-nebula-compose
```

## If something doesn't look right

1. Run this and copy the output:
   ```bash
   sudo journalctl -u mobility-nebula-compose.service -n 30
   docker ps -a
   ```
2. Send it to development team.

