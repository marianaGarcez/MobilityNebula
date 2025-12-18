#!/usr/bin/env python3
"""
Run the MobilityNebula EDBT query matrix inside Docker and
collect per-query metrics into a single JSON file.

This script assumes:
  - docker-compose.runtime.yaml is present at repo root.
  - The runtime image can be referenced via NES_RUNTIME_IMAGE
    (or the default in the compose file).
  - Query YAMLs live under Queries/ as in this repo.
  - Outputs are written to Output/output_query*.csv.

Example:
  python3 scripts/run_edbt_matrix.py \
    --compose-file docker-compose.runtime.yaml \
    --runtime-image marianamgarcez/mobility-nebula:runtime \
    --worker-threads 2 \
    --out Output/edbt/mobilitynebula_matrix_metrics.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


# Allow importing edbt_collect_metrics from the same directory.
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from edbt_collect_metrics import collect_metrics  # type: ignore[import]

_SOCKET_PORT_RE = re.compile(r'^(\s*socket_port\s*:\s*)(["\']?)(\d+)(["\']?)\s*$')


@dataclass
class QueryConfig:
    name: str
    query_file: Path
    output_csv: Path
    start_col: int
    end_col: int
    # Optional streaming (TCP) query variant
    tcp_query_file: Optional[Path] = None


def build_default_query_matrix(repo_root: Path) -> List[QueryConfig]:
    q = []
    # Q1–Q5: CSV variants, with separate TCP+CSV-sink variants where needed.
    q.append(QueryConfig(
        name="Q1",
        query_file=repo_root / "Queries" / "Query1-csv.yaml",
        output_csv=repo_root / "Output" / "output_query1.csv",
        start_col=0,
        end_col=1,
        # Use TCP source + CSV sink for streaming runs.
        tcp_query_file=repo_root / "Queries" / "Query1-tcp-file.yaml",
    ))
    q.append(QueryConfig(
        name="Q2",
        query_file=repo_root / "Queries" / "Query2-csv.yaml",
        output_csv=repo_root / "Output" / "output_query2.csv",
        start_col=0,
        end_col=1,
        tcp_query_file=repo_root / "Queries" / "Query2.yaml",
    ))
    q.append(QueryConfig(
        name="Q3",
        query_file=repo_root / "Queries" / "Query3-csv.yaml",
        output_csv=repo_root / "Output" / "output_query3.csv",
        start_col=0,
        end_col=1,
        tcp_query_file=repo_root / "Queries" / "Query3.yaml",
    ))
    q.append(QueryConfig(
        name="Q4",
        query_file=repo_root / "Queries" / "Query4-csv.yaml",
        output_csv=repo_root / "Output" / "output_query4.csv",
        start_col=0,
        end_col=1,
        tcp_query_file=repo_root / "Queries" / "Query4.yaml",
    ))
    q.append(QueryConfig(
        name="Q5",
        query_file=repo_root / "Queries" / "Query5-csv.yaml",
        output_csv=repo_root / "Output" / "output_query5.csv",
        start_col=1,  # device_id,start,end,...
        end_col=2,
        tcp_query_file=repo_root / "Queries" / "Query5.yaml",
    ))

    # Q6–Q9: CSV + TCP variants
    q.append(QueryConfig(
        name="Q6",
        query_file=repo_root / "Queries" / "Query6-csv.yaml",
        output_csv=repo_root / "Output" / "output_query6.csv",
        start_col=0,
        end_col=1,
        tcp_query_file=repo_root / "Queries" / "Query6.yaml",
    ))
    q.append(QueryConfig(
        name="Q7",
        query_file=repo_root / "Queries" / "Query7-csv.yaml",
        output_csv=repo_root / "Output" / "output_query7.csv",
        start_col=0,
        end_col=1,
        tcp_query_file=repo_root / "Queries" / "Query7.yaml",
    ))
    q.append(QueryConfig(
        name="Q8",
        query_file=repo_root / "Queries" / "Query8-csv.yaml",
        output_csv=repo_root / "Output" / "output_query8.csv",
        start_col=0,
        end_col=1,
        tcp_query_file=repo_root / "Queries" / "Query8.yaml",
    ))
    q.append(QueryConfig(
        name="Q9",
        query_file=repo_root / "Queries" / "Query9-csv.yaml",
        output_csv=repo_root / "Output" / "output_query9.csv",
        start_col=0,
        end_col=1,
        tcp_query_file=repo_root / "Queries" / "Query9.yaml",
    ))
    return q


def run_one_query(
    cfg: QueryConfig,
    compose_file: Path,
    runtime_image: Optional[str],
    worker_threads: int,
) -> Dict[str, Any]:
    """
    Run a single query via docker-compose.runtime.yaml and collect metrics.
    """
    if not cfg.query_file.is_file():
        return {
            "query": cfg.name,
            "error": f"query_file_not_found:{cfg.query_file}",
        }

    # Ensure output directory exists and previous file does not interfere.
    cfg.output_csv.parent.mkdir(parents=True, exist_ok=True)
    if cfg.output_csv.exists():
        cfg.output_csv.unlink()

    env = os.environ.copy()
    env["NES_QUERY_FILE"] = f"/workspace/Queries/{cfg.query_file.name}"
    env["NES_WORKER_THREADS"] = str(worker_threads)
    if runtime_image:
        env["NES_RUNTIME_IMAGE"] = runtime_image

    cmd = [
        "docker",
        "compose",
        "-f",
        str(compose_file),
        "up",
        "--force-recreate",
        "--abort-on-container-exit",
    ]

    start_time = time.monotonic()
    completed = subprocess.run(cmd, env=env, cwd=str(compose_file.parent))
    end_time = time.monotonic()
    run_seconds = end_time - start_time

    result: Dict[str, Any] = {
        "query": cfg.name,
        "query_file": str(cfg.query_file),
        "output_csv": str(cfg.output_csv),
        "run_seconds_wall": run_seconds,
        "docker_returncode": completed.returncode,
    }

    if completed.returncode != 0:
        result["error"] = "docker_compose_failed"
        return result

    if not cfg.output_csv.is_file():
        result["error"] = "output_csv_not_found"
        return result

    # For Q2 and Q5, you may later choose to pass non-zero processing/sink
    # constants based on microbenchmarks. For now, use defaults (0).
    metrics = collect_metrics(
        input_csv=cfg.output_csv,
        start_col=cfg.start_col,
        end_col=cfg.end_col,
        run_seconds=run_seconds,
        query_name=cfg.name,
    )
    result["metrics"] = metrics
    return result


def run_one_query_stream(
    cfg: QueryConfig,
    compose_file: Path,
    runtime_image: Optional[str],
    worker_threads: int,
    input_csv: Path,
    host: str,
    port: int,
    rows_per_sec: float,
    run_seconds: float,
    flush_seconds: float,
    batch_size: int,
    jitter_profile: Optional[str],
) -> Dict[str, Any]:
    """
    Run a single query in streaming mode:
      - Start MobilityNebula via docker compose with a TCP-based query
        (cfg.tcp_query_file, or cfg.query_file if tcp variant is not set).
      - Drive load using tcp_source_csv_server.py at the given rate.
      - After run_seconds, stop the generator, wait flush_seconds, bring down
        the compose stack, and compute simple metrics from the sink CSV.
    """
    tcp_query = cfg.tcp_query_file or cfg.query_file
    if not tcp_query.is_file():
        return {
            "query": cfg.name,
            "error": f"tcp_query_file_not_found:{tcp_query}",
        }

    if not input_csv.is_file():
        return {
            "query": cfg.name,
            "error": f"input_csv_not_found:{input_csv}",
        }

    def pick_free_port(preferred: int) -> int:
        if preferred > 0:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind(("0.0.0.0", preferred))
                return preferred
            except OSError:
                pass
            finally:
                s.close()

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind(("0.0.0.0", 0))
            return int(s.getsockname()[1])
        finally:
            s.close()

    def materialize_tcp_query(src: Path, chosen_port: int) -> Path:
        tmp_dir = src.parent / "_tmp"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        dst = tmp_dir / f"{src.stem}_port{chosen_port}{src.suffix}"

        replaced = False
        out_lines: list[str] = []
        for line in src.read_text(encoding="utf-8").splitlines(keepends=False):
            m = _SOCKET_PORT_RE.match(line)
            if m:
                prefix, q1, _, q2 = m.groups()
                quote = q1 or q2 or '"'
                out_lines.append(f"{prefix}{quote}{chosen_port}{quote}")
                replaced = True
            else:
                out_lines.append(line)

        if not replaced:
            raise ValueError(f"socket_port not found in {src}")

        dst.write_text("\n".join(out_lines) + "\n", encoding="utf-8")
        return dst

    def docker_compose_service_id(service: str) -> str:
        try:
            out = subprocess.check_output(
                [
                    "docker",
                    "compose",
                    "-f",
                    str(compose_file),
                    "ps",
                    "--all",
                    "-q",
                    service,
                ],
                cwd=str(compose_file.parent),
                env=env,
            ).decode("utf-8", errors="replace")
            return out.strip()
        except Exception:
            return ""

    def docker_logs(container_id: str) -> str:
        if not container_id:
            return ""
        try:
            return subprocess.check_output(
                ["docker", "logs", "--tail", "200", container_id],
                stderr=subprocess.STDOUT,
            ).decode("utf-8", errors="replace")
        except Exception:
            return ""

    # Ensure output directory exists and previous file does not interfere.
    cfg.output_csv.parent.mkdir(parents=True, exist_ok=True)
    if cfg.output_csv.exists():
        cfg.output_csv.unlink()

    env = os.environ.copy()
    env["NES_WORKER_THREADS"] = str(worker_threads)
    env["NES_WAIT_FOR_QUERY"] = "0"
    if runtime_image:
        env["NES_RUNTIME_IMAGE"] = runtime_image

    chosen_port = pick_free_port(port)
    tcp_query_materialized = materialize_tcp_query(tcp_query, chosen_port)

    # Start load generator early so the TCP source can connect immediately when the query starts.
    # (Some TCP sources do not retry on initial connection failure.)
    server_script = Path(__file__).resolve().parent / "tcp_source_csv_server.py"
    run_log_dir = cfg.output_csv.parent / "run_matrix"
    run_log_dir.mkdir(parents=True, exist_ok=True)
    source_log = run_log_dir / f"{cfg.name}_tcp_source.log"
    source_log.write_text("", encoding="utf-8")

    def start_load_proc(rps: float) -> subprocess.Popen:
        cmd = [
            sys.executable,
            "-u",
            str(server_script),
            str(input_csv),
            "--host",
            host,
            "--port",
            str(chosen_port),
            "--rows-per-sec",
            str(rps),
            "--batch-size",
            str(batch_size),
            "--loop",
            "--log-connections",
            # Ensure timestamps keep increasing even when looping and across duplicates.
            # Also avoid dropping events from different devices that share the same timestamp.
            "--order-scope",
            "per-key",
            "--key-col-index",
            "1",
            "--repair-monotonic-seconds",
            "1",
        ]
        f = source_log.open("a", encoding="utf-8")
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        proc = subprocess.Popen(cmd, stdout=f, stderr=f, env=env)
        f.close()
        return proc

    initial_rps = rows_per_sec
    if jitter_profile:
        # Keep a stable connection for now by running the first phase rate for the full duration.
        # (Restarting the TCP server between phases can drop the connection and yield empty output.)
        try:
            first_seg = jitter_profile.split(";", 1)[0].strip()
            if first_seg:
                initial_rps = float(first_seg.split(",", 1)[0].strip()) or rows_per_sec
        except Exception:
            initial_rps = rows_per_sec

    load_proc: subprocess.Popen | None = None
    try:
        load_proc = start_load_proc(initial_rps)
    except Exception as exc:  # pylint: disable=broad-except
        return {
            "query": cfg.name,
            "tcp_query_file": str(tcp_query_materialized),
            "error": f"failed_to_start_load_generator:{exc}",
            "tcp_source_log": str(source_log),
            "tcp_port": chosen_port,
        }

    # If the server failed to bind/crashed immediately, bail out early with its log.
    time.sleep(0.25)
    if load_proc.poll() is not None:
        return {
            "query": cfg.name,
            "tcp_query_file": str(tcp_query_materialized),
            "error": f"tcp_source_exited_early:{load_proc.returncode}",
            "tcp_source_log": str(source_log),
            "tcp_port": chosen_port,
        }

    # Start the stack in the background.
    up_cmd = [
        "docker",
        "compose",
        "-f",
        str(compose_file),
        "up",
        "-d",
        "--force-recreate",
    ]
    env["NES_QUERY_FILE"] = f"/workspace/Queries/{tcp_query_materialized.parent.name}/{tcp_query_materialized.name}"
    up_res = subprocess.run(up_cmd, env=env, cwd=str(compose_file.parent))
    if up_res.returncode != 0:
        if load_proc is not None:
            load_proc.terminate()
            try:
                load_proc.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                load_proc.kill()
                load_proc.wait(timeout=5.0)
        return {
            "query": cfg.name,
            "tcp_query_file": str(tcp_query_materialized),
            "error": "docker_compose_up_failed",
            "docker_returncode": up_res.returncode,
        }

    # Ensure query registration finished before sending data.
    query_reg_id = ""
    deadline = time.time() + 60.0
    while time.time() < deadline:
        query_reg_id = docker_compose_service_id("query-registration")
        if query_reg_id:
            break
        time.sleep(0.5)

    if not query_reg_id:
        subprocess.run(
            ["docker", "compose", "-f", str(compose_file), "down", "--remove-orphans"],
            env=env,
            cwd=str(compose_file.parent),
        )
        if load_proc is not None:
            load_proc.terminate()
            try:
                load_proc.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                load_proc.kill()
                load_proc.wait(timeout=5.0)
        return {
            "query": cfg.name,
            "tcp_query_file": str(tcp_query_materialized),
            "error": "query_registration_container_not_found",
        }

    try:
        wait_res = subprocess.run(["docker", "wait", query_reg_id], timeout=90.0, capture_output=True)
    except subprocess.TimeoutExpired:
        wait_res = None

    if wait_res is None or wait_res.returncode != 0 or wait_res.stdout.strip() not in {b"0", b"0\n"}:
        qr_logs = docker_logs(query_reg_id)
        worker_id = docker_compose_service_id("nes-worker")
        worker_logs = docker_logs(worker_id)
        subprocess.run(
            ["docker", "compose", "-f", str(compose_file), "down", "--remove-orphans"],
            env=env,
            cwd=str(compose_file.parent),
        )
        if load_proc is not None:
            load_proc.terminate()
            try:
                load_proc.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                load_proc.kill()
                load_proc.wait(timeout=5.0)
        return {
            "query": cfg.name,
            "tcp_query_file": str(tcp_query_materialized),
            "error": "query_registration_failed",
            "query_registration_logs_tail": qr_logs,
            "nes_worker_logs_tail": worker_logs,
        }

    # Sanity check: ensure the worker actually connected to the TCP source. If we never got
    # a connection, the output will likely be empty (header-only).
    connected_deadline = time.time() + 15.0
    while time.time() < connected_deadline:
        try:
            if "Client connected" in source_log.read_text(encoding="utf-8", errors="replace"):
                break
        except Exception:
            pass
        time.sleep(0.25)
    else:
        subprocess.run(
            ["docker", "compose", "-f", str(compose_file), "down", "--remove-orphans"],
            env=env,
            cwd=str(compose_file.parent),
        )
        if load_proc is not None:
            load_proc.terminate()
            try:
                load_proc.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                load_proc.kill()
                load_proc.wait(timeout=5.0)
        return {
            "query": cfg.name,
            "tcp_query_file": str(tcp_query_materialized),
            "error": "tcp_source_never_connected",
            "tcp_source_log": str(source_log),
            "tcp_port": chosen_port,
        }

    load_start = time.monotonic()

    time.sleep(run_seconds)
    if load_proc is not None:
        load_proc.terminate()
        try:
            load_proc.wait(timeout=10.0)
        except subprocess.TimeoutExpired:
            load_proc.kill()
            load_proc.wait(timeout=5.0)

    load_end = time.monotonic()
    effective_run_seconds = load_end - load_start

    # Allow some time for windows/sinks to flush.
    if flush_seconds > 0:
        time.sleep(flush_seconds)

    # Stop the stack.
    subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "down", "--remove-orphans"],
        env=env,
        cwd=str(compose_file.parent),
    )

    result: Dict[str, Any] = {
        "query": cfg.name,
        "tcp_query_file": str(tcp_query_materialized),
        "output_csv": str(cfg.output_csv),
        "run_seconds_wall": effective_run_seconds,
        "tcp_source_log": str(source_log),
        "tcp_port": chosen_port,
    }

    if not cfg.output_csv.is_file():
        result["error"] = "output_csv_not_found"
        return result

    metrics = collect_metrics(
        input_csv=cfg.output_csv,
        start_col=cfg.start_col,
        end_col=cfg.end_col,
        run_seconds=effective_run_seconds,
        query_name=f"{cfg.name}-stream",
    )
    result["metrics"] = metrics
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run MobilityNebula EDBT query matrix inside Docker "
                    "and collect per-query metrics."
    )
    parser.add_argument("--compose-file", type=str, default="docker-compose.runtime.yaml",
                        help="Path to docker-compose runtime file")
    parser.add_argument("--runtime-image", type=str, default=None,
                        help="NES runtime image (overrides NES_RUNTIME_IMAGE env)")
    parser.add_argument("--worker-threads", type=int, default=2,
                        help="NES_WORKER_THREADS for nes-worker container")
    parser.add_argument("--mode", type=str, choices=["csv", "stream"], default="csv",
                        help="Execution mode: 'csv' for file-based runs, "
                             "'stream' to drive tcp_source_csv_server at a fixed rate")
    parser.add_argument("--stream-input-csv", type=str, default="Input/input_sncb.csv",
                        help="Input CSV path for streaming mode")
    parser.add_argument("--stream-host", type=str, default="0.0.0.0",
                        help="Host/IP to bind tcp_source_csv_server in streaming mode "
                             "(0.0.0.0 recommended so Nes worker can reach it via "
                             "host.docker.internal)")
    parser.add_argument("--stream-port", type=int, default=32324,
                        help="Port for tcp_source_csv_server in streaming mode")
    parser.add_argument("--stream-rows-per-sec", type=float, default=20000.0,
                        help="Rows per second for streaming mode (e.g., 20000)")
    parser.add_argument("--stream-run-seconds", type=float, default=30.0,
                        help="Wall-clock duration per streaming run (seconds)")
    parser.add_argument("--stream-flush-seconds", type=float, default=5.0,
                        help="Extra time after stopping load to allow windows/sinks to flush")
    parser.add_argument("--stream-batch-size", type=int, default=100,
                        help="Batch size for tcp_source_csv_server in streaming mode "
                             "(use 1000 for a high-burst profile)")
    parser.add_argument("--stream-jitter-profile", type=str, default="",
                        help="Optional jitter profile, e.g. '10000,10;20000,10;40000,10' "
                             "for 10k/20k/40k e/s phases of 10s each. "
                             "If set, overrides --stream-rows-per-sec/--stream-run-seconds.")
    parser.add_argument("--queries", nargs="*", default=None,
                        help="Optional subset of queries to run (e.g., Q2 Q5). "
                             "Default: all Q1..Q9.")
    parser.add_argument("--out", type=str, default="Output/edbt/mobilitynebula_matrix_metrics.json",
                        help="Path to consolidated metrics JSON")

    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    compose_file = repo_root / args.compose_file
    if not compose_file.is_file():
        print(f"[ERROR] compose file not found: {compose_file}", file=sys.stderr)
        sys.exit(1)

    all_cfgs = build_default_query_matrix(repo_root)
    if args.queries:
        wanted = set(args.queries)
        cfgs = [c for c in all_cfgs if c.name in wanted]
    else:
        cfgs = all_cfgs

    consolidated: Dict[str, Any] = {
        "repo_root": str(repo_root),
        "compose_file": str(compose_file),
        "worker_threads": args.worker_threads,
        "runtime_image": args.runtime_image,
        "queries": {},
    }

    for cfg in cfgs:
        if args.mode == "csv":
            print(f"[INFO] Running {cfg.name} in CSV mode using {cfg.query_file} ...")
            res = run_one_query(
                cfg=cfg,
                compose_file=compose_file,
                runtime_image=args.runtime_image,
                worker_threads=args.worker_threads,
            )
        else:
            print(f"[INFO] Running {cfg.name} in STREAM mode using "
                  f"{cfg.tcp_query_file or cfg.query_file} ...")
            res = run_one_query_stream(
                cfg=cfg,
                compose_file=compose_file,
                runtime_image=args.runtime_image,
                worker_threads=args.worker_threads,
                input_csv=repo_root / args.stream_input_csv,
                host=args.stream_host,
                port=args.stream_port,
                rows_per_sec=args.stream_rows_per_sec,
                run_seconds=args.stream_run_seconds,
                flush_seconds=args.stream_flush_seconds,
                batch_size=args.stream_batch_size,
                jitter_profile=args.stream_jitter_profile,
            )
        consolidated["queries"][cfg.name] = res

    out_path = repo_root / args.out
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(consolidated, f, indent=2, sort_keys=True)

    print(f"[INFO] Wrote consolidated metrics to {out_path}")


if __name__ == "__main__":
    main()
