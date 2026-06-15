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

`NES_QUERY_FILE` is just the **file name** (no path). The yaml must already
exist in the queries directory — see below.

## To add a new query

Query yamls live on the nrok in `/opt/mobility-nebula/Queries/` (this folder is
mounted into the containers at `/workspace/Queries`). To add your own:

1. **Write the yaml.** Easiest is to copy an existing one as a starting point:

   ```bash
   sudo cp /opt/mobility-nebula/Queries/sncb_brake_monitoring.yaml \
           /opt/mobility-nebula/Queries/my_query.yaml
   sudo nano /opt/mobility-nebula/Queries/my_query.yaml
   ```

   A query yaml has four parts. The input is a **TCP CSV stream** (the worker
   connects out to the GPS/brake feed on `host.docker.internal:9000`) and the
   output is an **MQTT sink** (to `mosquitto:1883`):

   ```yaml
   query: |
     SELECT start, end, device_id, VAR(PCFA_mbar) AS PCFA_var
     FROM gps_data
     GROUP BY device_id
     WINDOW SLIDING(time_utc, SIZE 30 SEC, ADVANCE BY 10 SEC)
     INTO brake_alerts_stream;

   sinks:                                   # OUTPUT: MQTT
     - name: brake_alerts_stream
       type: MQTT
       schema:                              # must match the SELECT output, in order.
         - { name: GPS_DATA$START,     type: UINT64  }   # prefix every field GPS_DATA$
         - { name: GPS_DATA$END,       type: UINT64  }
         - { name: GPS_DATA$DEVICE_ID, type: UINT64  }
         - { name: GPS_DATA$PCFA_VAR,  type: FLOAT64 }
       config:
         serverURI:    "tcp://mosquitto:1883"
         clientId:     "nebulastream_brake_monitor"
         topic:        "sncb/brake/alerts"   # the topic alerts are published to
         qos:          1
         cleanSession: true
         inputFormat:  CSV

   logical:                                 # the gps_data field layout (14 CSV columns)
     - name: gps_data
       schema:
         - { name: time_utc,  type: UINT64  }
         - { name: device_id, type: UINT64  }
         - { name: Vbat,      type: FLOAT64 }
         - { name: PCFA_mbar, type: FLOAT64 }
         - { name: PCFF_mbar, type: FLOAT64 }
         - { name: PCF1_mbar, type: FLOAT64 }
         - { name: PCF2_mbar, type: FLOAT64 }
         - { name: T1_mbar,   type: FLOAT64 }
         - { name: T2_mbar,   type: FLOAT64 }
     - logical: gps_data
       type: TCP
       parser_config:
         type: CSV
         field_delimiter: ","
         tuple_delimiter: "\n"
       source_config:
         socket_host: "host.docker.internal"
         socket_port: "9000"
         flush_interval_ms: "100"
         connect_timeout_ms: "5000"
         auto_reconnect: "true"
   ```

   Usually you only change the `query:` block and the sink `schema:` (the sink
   schema must list the same fields the `SELECT` produces, in the same order,
   each prefixed with `GPS_DATA$`).

2. **Point the stack at it** (file name only, no path):

   ```bash
   sudo sed -i 's/^NES_QUERY_FILE=.*/NES_QUERY_FILE=my_query.yaml/' \
       /etc/mobility-nebula/compose.env
   ```

3. **Restart:**

   ```bash
   sudo systemctl restart mobility-nebula-compose
   ```

4. **Confirm it registered:**

   ```bash
   sudo docker logs --tail 40 mobility-nebula-query-registration-1
   ```

   You should see the operator plan, the line
   `Query submitted from /workspace/Queries/my_query.yaml`, and
   `started successfully`.

Alerts are published to the MQTT topic in your sink (`sncb/brake/alerts` above).
To watch them:

```bash
sudo docker exec -it mosquitto mosquitto_sub -t 'sncb/brake/alerts' -v
```

## If something doesn't look right

1. Run this and copy the output:
   ```bash
   sudo journalctl -u mobility-nebula-compose.service -n 30
   docker ps -a
   ```
2. Send it to development team.

