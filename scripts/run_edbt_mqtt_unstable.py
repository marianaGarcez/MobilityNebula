#!/usr/bin/env python3
"""
Run MQTT-based unstable-network experiments for MobilityNebula.

This script:
  - Starts the MobilityNebula runtime + Mosquitto via docker-compose.runtime.yaml.
  - Deploys a query with both File + MQTT sinks (e.g., Query2-mqtt.yaml / Query5-mqtt.yaml).
  - Drives a TCP CSV source at a fixed rate using tcp_source_csv_server.py.
  - Pauses and resumes the Mosquitto container to simulate broker outages.
  - Subscribes to the query's MQTT topic to count delivered alerts.
  - Parses the file sink output to count produced alerts and basic metrics.
  - Writes a JSON summary to Output/edbt/mobilitynebula_mqtt_unstable_*.json.

Example:
  python3 scripts/run_edbt_mqtt_unstable.py \\
    --query Q2 \\
    --compose-file docker-compose.runtime.yaml \\
    --runtime-image marianamgarcez/mobility-nebula:runtime \\
    --worker-threads 2 \\
    --rows-per-sec 20000 \\
    --batch-size 1000 \\
    --run-seconds 60 \\
    --outage-start 20 \\
    --outage-duration 10 \\
    --out Output/edbt/mobilitynebula_mqtt_unstable_q2.json
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

try:
    import paho.mqtt.client as mqtt  # type: ignore[import]
except ImportError as exc:  # pragma: no cover - import-time guard
    print(
        "[ERROR] paho-mqtt is required for MQTT experiments.\n"
        "Install it with:\n"
        "  pip install paho-mqtt",
        file=sys.stderr,
    )
    raise

# Allow importing edbt_collect_metrics from the same directory.
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from edbt_collect_metrics import collect_metrics, _percentile  # type: ignore[import]


@dataclass
class QueryMqttConfig:
    name: str
    query_file: Path
    output_csv: Path
    mqtt_topic: str
    start_col: int
    end_col: int


@dataclass
class MqttMetrics:
    total_messages: int = 0
    before_outage: int = 0
    during_outage: int = 0
    after_outage: int = 0
    first_message_ts: Optional[float] = None
    first_after_outage_ts: Optional[float] = None
    last_before_outage_ts: Optional[float] = None
    # Per-message latency breakdown (based on MQTT payload).
    window_wait_ms: list[float] = None  # type: ignore[assignment]
    total_latency_ms: list[float] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        # Lazily initialize lists to keep the dataclass simple.
        if self.window_wait_ms is None:
            self.window_wait_ms = []
        if self.total_latency_ms is None:
            self.total_latency_ms = []


def build_query_configs(repo_root: Path) -> Dict[str, QueryMqttConfig]:
    """Return MQTT-enabled query configs for unstable-network experiments."""
    return {
        "Q2": QueryMqttConfig(
            name="Q2",
            query_file=repo_root / "Queries" / "Query2-mqtt.yaml",
            output_csv=repo_root / "Output" / "output_query2_mqtt.csv",
            mqtt_topic="sncb/query2/alerts",
            start_col=0,
            end_col=1,
        ),
        "Q5": QueryMqttConfig(
            name="Q5",
            query_file=repo_root / "Queries" / "Query5-mqtt.yaml",
            output_csv=repo_root / "Output" / "output_query5_mqtt.csv",
            mqtt_topic="sncb/query5/alerts",
            start_col=1,  # device_id,start,end,...
            end_col=2,
        ),
    }


def run_experiment(
    cfg: QueryMqttConfig,
    compose_file: Path,
    runtime_image: Optional[str],
    worker_threads: int,
    input_csv: Path,
    tcp_host: str,
    tcp_port: int,
    rows_per_sec: float,
    batch_size: int,
    run_seconds: float,
    outage_start: float,
    outage_duration: float,
    mosquitto_service: str,
) -> Dict[str, Any]:
    """
    Run a single unstable-network MQTT experiment for one query.
    """
    if not cfg.query_file.is_file():
        return {
            "query": cfg.name,
            "error": f"query_file_not_found:{cfg.query_file}",
        }

    if not input_csv.is_file():
        return {
            "query": cfg.name,
            "error": f"input_csv_not_found:{input_csv}",
        }

    if not compose_file.is_file():
        return {
            "query": cfg.name,
            "error": f"compose_file_not_found:{compose_file}",
        }

    # Prepare output CSV.
    cfg.output_csv.parent.mkdir(parents=True, exist_ok=True)
    if cfg.output_csv.exists():
        cfg.output_csv.unlink()

    # Environment for docker compose.
    env = os.environ.copy()
    env["NES_QUERY_FILE"] = f"/workspace/Queries/{cfg.query_file.name}"
    env["NES_WORKER_THREADS"] = str(worker_threads)
    if runtime_image:
        env["NES_RUNTIME_IMAGE"] = runtime_image

    # Start the full stack in the background (nes-worker, query-registration, mosquitto).
    up_cmd = [
        "docker",
        "compose",
        "-f",
        str(compose_file),
        "up",
        "-d",
    ]

    up_res = subprocess.run(up_cmd, env=env)
    if up_res.returncode != 0:
        return {
            "query": cfg.name,
            "query_file": str(cfg.query_file),
            "error": "docker_compose_up_failed",
            "docker_returncode": up_res.returncode,
        }

    # Give nes-worker + query-registration a moment to come up.
    time.sleep(5.0)

    experiment_start = time.monotonic()
    outage_start_ts = experiment_start + max(0.0, outage_start)
    outage_end_ts = outage_start_ts + max(0.0, outage_duration)

    mqtt_metrics = MqttMetrics()
    metrics_lock = threading.Lock()

    def on_connect(client: mqtt.Client, _userdata, _flags, rc: int) -> None:
        if rc == 0:
            client.subscribe(cfg.mqtt_topic)
        else:
            print(f"[WARN] MQTT connect returned code {rc}", file=sys.stderr)

    def on_message(client: mqtt.Client, _userdata, msg: mqtt.MQTTMessage) -> None:
        now_monotonic = time.monotonic()
        now_wall = time.time()
        with metrics_lock:
            mqtt_metrics.total_messages += 1
            if mqtt_metrics.first_message_ts is None:
                mqtt_metrics.first_message_ts = now_monotonic

            if now_monotonic < outage_start_ts:
                mqtt_metrics.before_outage += 1
                mqtt_metrics.last_before_outage_ts = now_monotonic
            elif outage_start_ts <= now_monotonic <= outage_end_ts:
                mqtt_metrics.during_outage += 1
            else:
                mqtt_metrics.after_outage += 1
                if mqtt_metrics.first_after_outage_ts is None:
                    mqtt_metrics.first_after_outage_ts = now_monotonic

            # Try to parse payload as CSV to extract start/end event-time bounds.
            try:
                payload = msg.payload.decode().strip()
                if not payload:
                    return
                cols = payload.split(",")
                if cfg.start_col >= len(cols) or cfg.end_col >= len(cols):
                    return
                start = int(cols[cfg.start_col])
                end = int(cols[cfg.end_col])
            except Exception:
                # If parsing fails, skip latency computation for this message.
                return

            duration_ms = float(max(0, end - start)) * 1000.0
            wait_ms = duration_ms / 2.0
            latency_ms = float(max(0.0, now_wall - end)) * 1000.0

            mqtt_metrics.window_wait_ms.append(wait_ms)
            mqtt_metrics.total_latency_ms.append(latency_ms)

    def on_disconnect(client: mqtt.Client, _userdata, rc: int) -> None:
        if rc != 0:
            print("[WARN] Unexpected MQTT disconnection", file=sys.stderr)

    # Start MQTT subscriber on host, connecting to mapped Mosquitto port.
    mqtt_client = mqtt.Client(client_id=f"mqtt_unstable_{cfg.name}")
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.connect("localhost", 1883, keepalive=30)
    mqtt_client.loop_start()

    # Prepare TCP load generator command.
    server_script = SCRIPT_DIR / "tcp_source_csv_server.py"

    gen_cmd = [
        sys.executable,
        str(server_script),
        str(input_csv),
        "--host",
        tcp_host,
        "--port",
        str(tcp_port),
        "--rows-per-sec",
        str(rows_per_sec),
        "--batch-size",
        str(batch_size),
        "--loop",
    ]

    try:
        generator_proc = subprocess.Popen(gen_cmd)
    except Exception as exc:  # pylint: disable=broad-except
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        subprocess.run(
            ["docker", "compose", "-f", str(compose_file), "down", "--remove-orphans"],
            env=env,
        )
        return {
            "query": cfg.name,
            "query_file": str(cfg.query_file),
            "error": f"failed_to_start_load_generator:{exc}",
        }

    def outage_worker() -> None:
        # Sleep until outage start, then pause/unpause Mosquitto.
        while True:
            now = time.monotonic()
            if now >= outage_start_ts:
                break
            time.sleep(0.1)

        pause_cmd = [
            "docker",
            "pause",
            mosquitto_service,
        ]
        subprocess.run(pause_cmd)

        while True:
            now = time.monotonic()
            if now >= outage_end_ts:
                break
            time.sleep(0.1)

        unpause_cmd = [
            "docker",
            "unpause",
            mosquitto_service,
        ]
        subprocess.run(unpause_cmd)

    outage_thread = threading.Thread(target=outage_worker, daemon=True)
    outage_thread.start()

    # Let the experiment run.
    time.sleep(run_seconds)

    # Stop generator.
    generator_proc.terminate()
    try:
        generator_proc.wait(timeout=10.0)
    except subprocess.TimeoutExpired:
        generator_proc.kill()
        generator_proc.wait(timeout=5.0)

    experiment_end = time.monotonic()
    effective_run_seconds = experiment_end - experiment_start

    # Stop MQTT client.
    mqtt_client.loop_stop()
    mqtt_client.disconnect()

    # Allow windows/sinks to flush a bit.
    time.sleep(5.0)

    # Tear down the stack.
    subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "down", "--remove-orphans"],
        env=env,
    )

    result: Dict[str, Any] = {
        "query": cfg.name,
        "query_file": str(cfg.query_file),
        "output_csv": str(cfg.output_csv),
        "run_seconds_wall": effective_run_seconds,
        "rows_per_sec": rows_per_sec,
        "batch_size": batch_size,
        "outage": {
            "start_offset_s": outage_start,
            "duration_s": outage_duration,
        },
    }

    # MQTT metrics.
    with metrics_lock:
        base = {
            "total_messages": mqtt_metrics.total_messages,
            "before_outage": mqtt_metrics.before_outage,
            "during_outage": mqtt_metrics.during_outage,
            "after_outage": mqtt_metrics.after_outage,
        }

        def rel(ts: Optional[float]) -> Optional[float]:
            if ts is None:
                return None
            return float(ts - experiment_start)

        base["first_message_time_s"] = rel(mqtt_metrics.first_message_ts)
        base["last_before_outage_time_s"] = rel(mqtt_metrics.last_before_outage_ts)
        base["first_after_outage_time_s"] = rel(mqtt_metrics.first_after_outage_ts)

    mqtt_latency = {}
    if mqtt_metrics.total_latency_ms and mqtt_metrics.window_wait_ms:
        # Processing = max(0, L - window_wait), sink treated as residual (0 here).
        proc_vals = [
            max(0.0, l - w)
            for l, w in zip(mqtt_metrics.total_latency_ms, mqtt_metrics.window_wait_ms)
        ]
        sink_vals = [0.0] * len(proc_vals)
        mqtt_latency = {
            "p50_total": _percentile(mqtt_metrics.total_latency_ms, 50.0),
            "p95_total": _percentile(mqtt_metrics.total_latency_ms, 95.0),
            "p50_window_wait": _percentile(mqtt_metrics.window_wait_ms, 50.0),
            "p95_window_wait": _percentile(mqtt_metrics.window_wait_ms, 95.0),
            "p50_processing": _percentile(proc_vals, 50.0),
            "p95_processing": _percentile(proc_vals, 95.0),
            "p50_sink": _percentile(sink_vals, 50.0),
            "p95_sink": _percentile(sink_vals, 95.0),
        }

    result["mqtt"] = {
        "topic": cfg.mqtt_topic,
        **base,
        "latency_ms": mqtt_latency,
    }

    # Production (file sink) metrics.
    if cfg.output_csv.is_file():
        metrics = collect_metrics(
            input_csv=cfg.output_csv,
            start_col=cfg.start_col,
            end_col=cfg.end_col,
            run_seconds=effective_run_seconds,
            query_name=f"{cfg.name}-mqtt",
        )
        result["production_metrics"] = metrics
    else:
        result["production_metrics"] = {
            "error": "output_csv_not_found",
            "path": str(cfg.output_csv),
        }

    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run MQTT unstable-network experiments for MobilityNebula."
    )
    parser.add_argument(
        "--compose-file",
        type=str,
        default="docker-compose.runtime.yaml",
        help="Path to docker-compose runtime file (must include mosquitto service)",
    )
    parser.add_argument(
        "--runtime-image",
        type=str,
        default=None,
        help="NES runtime image (overrides NES_RUNTIME_IMAGE env)",
    )
    parser.add_argument(
        "--worker-threads",
        type=int,
        default=2,
        help="NES_WORKER_THREADS for nes-worker container",
    )
    parser.add_argument(
        "--query",
        type=str,
        choices=["Q2", "Q5"],
        default="Q2",
        help="Which MQTT-enabled query to run (Q2 or Q5)",
    )
    parser.add_argument(
        "--input-csv",
        type=str,
        default="Input/input_sncb.csv",
        help="Input CSV path for TCP generator (host path, relative to repo root)",
    )
    parser.add_argument(
        "--tcp-host",
        type=str,
        default="0.0.0.0",
        help="Host/IP for tcp_source_csv_server to bind (0.0.0.0 so nes-worker can reach via host.docker.internal)",
    )
    parser.add_argument(
        "--tcp-port",
        type=int,
        default=32324,
        help="Port for tcp_source_csv_server (must match TCP source in query YAML)",
    )
    parser.add_argument(
        "--rows-per-sec",
        type=float,
        default=20000.0,
        help="Rows per second for TCP generator (e.g., 20000)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Batch size for tcp_source_csv_server (use 1000 for high-burst profile)",
    )
    parser.add_argument(
        "--run-seconds",
        type=float,
        default=60.0,
        help="Total experiment duration in seconds (including outage)",
    )
    parser.add_argument(
        "--outage-start",
        type=float,
        default=20.0,
        help="Seconds after experiment start to pause Mosquitto",
    )
    parser.add_argument(
        "--outage-duration",
        type=float,
        default=10.0,
        help="Duration of Mosquitto pause in seconds",
    )
    parser.add_argument(
        "--mosquitto-service",
        type=str,
        default="mosquitto",
        help="Name of the Mosquitto service/container for docker pause/unpause",
    )
    parser.add_argument(
        "--out",
        type=str,
        default=None,
        help="Optional JSON output path (default: Output/edbt/mobilitynebula_mqtt_unstable_<query>.json)",
    )

    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    compose_file = repo_root / args.compose_file

    cfgs = build_query_configs(repo_root)
    if args.query not in cfgs:
        print(f"[ERROR] Unsupported query: {args.query}", file=sys.stderr)
        sys.exit(1)

    cfg = cfgs[args.query]

    result = run_experiment(
        cfg=cfg,
        compose_file=compose_file,
        runtime_image=args.runtime_image,
        worker_threads=args.worker_threads,
        input_csv=repo_root / args.input_csv,
        tcp_host=args.tcp_host,
        tcp_port=args.tcp_port,
        rows_per_sec=args.rows_per_sec,
        batch_size=args.batch_size,
        run_seconds=args.run_seconds,
        outage_start=args.outage_start,
        outage_duration=args.outage_duration,
        mosquitto_service=args.mosquitto_service,
    )

    # Attach basic run configuration for reproducibility.
    summary: Dict[str, Any] = {
        "query": cfg.name,
        "query_file": str(cfg.query_file),
        "compose_file": str(compose_file),
        "runtime_image": args.runtime_image,
        "worker_threads": args.worker_threads,
        "input_csv": str(repo_root / args.input_csv),
        "rows_per_sec": args.rows_per_sec,
        "batch_size": args.batch_size,
        "run_seconds": args.run_seconds,
        "outage_start": args.outage_start,
        "outage_duration": args.outage_duration,
        "mosquitto_service": args.mosquitto_service,
        "result": result,
    }

    if args.out:
        out_path = repo_root / args.out
    else:
        out_dir = repo_root / "Output" / "edbt"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"mobilitynebula_mqtt_unstable_{cfg.name.lower()}.json"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, sort_keys=True)

    print(f"[INFO] Wrote MQTT unstable-network metrics to {out_path}")


if __name__ == "__main__":
    main()
