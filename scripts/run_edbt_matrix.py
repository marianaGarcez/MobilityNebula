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
    # Q1–Q5: CSV variants
    q.append(QueryConfig(
        name="Q1",
        query_file=repo_root / "Queries" / "Query1-csv.yaml",
        output_csv=repo_root / "Output" / "output_query1.csv",
        start_col=0,
        end_col=1,
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
    ))
    q.append(QueryConfig(
        name="Q4",
        query_file=repo_root / "Queries" / "Query4-csv.yaml",
        output_csv=repo_root / "Output" / "output_query4.csv",
        start_col=0,
        end_col=1,
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
    completed = subprocess.run(cmd, env=env)
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

    # Ensure output directory exists and previous file does not interfere.
    cfg.output_csv.parent.mkdir(parents=True, exist_ok=True)
    if cfg.output_csv.exists():
        cfg.output_csv.unlink()

    env = os.environ.copy()
    env["NES_QUERY_FILE"] = f"/workspace/Queries/{tcp_query.name}"
    env["NES_WORKER_THREADS"] = str(worker_threads)
    if runtime_image:
        env["NES_RUNTIME_IMAGE"] = runtime_image

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
    up_res = subprocess.run(up_cmd, env=env)
    if up_res.returncode != 0:
        return {
            "query": cfg.name,
            "tcp_query_file": str(tcp_query),
            "error": "docker_compose_up_failed",
            "docker_returncode": up_res.returncode,
        }

    # Give nes-worker + query-registration a moment to come up.
    time.sleep(5.0)

    # Start load generator (looping to sustain run_seconds), optionally
    # with a jitter profile of multiple phases.
    server_script = Path(__file__).resolve().parent / "tcp_source_csv_server.py"
    def start_load_proc(rps: float) -> subprocess.Popen:
        cmd = [
            sys.executable,
            str(server_script),
            str(input_csv),
            "--host",
            host,
            "--port",
            str(port),
            "--rows-per-sec",
            str(rps),
            "--batch-size",
            str(batch_size),
            "--loop",
        ]
        return subprocess.Popen(cmd)

    load_start = time.monotonic()

    if jitter_profile:
        # Profile format: "10000,10;20000,10;40000,10"
        phases: List[tuple[float, float]] = []
        for segment in jitter_profile.split(";"):
            segment = segment.strip()
            if not segment:
                continue
            parts = segment.split(",")
            if len(parts) != 2:
                continue
            try:
                rps = float(parts[0])
                dur = float(parts[1])
            except ValueError:
                continue
            if rps <= 0 or dur <= 0:
                continue
            phases.append((rps, dur))

        if not phases:
            phases.append((rows_per_sec, run_seconds))

        for rps, dur in phases:
            try:
                proc = start_load_proc(rps)
            except Exception as exc:  # pylint: disable=broad-except
                subprocess.run(
                    ["docker", "compose", "-f", str(compose_file), "down", "--remove-orphans"],
                    env=env,
                )
                return {
                    "query": cfg.name,
                    "tcp_query_file": str(tcp_query),
                    "error": f"failed_to_start_load_generator:{exc}",
                }
            time.sleep(dur)
            proc.terminate()
            try:
                proc.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5.0)
    else:
        try:
            proc = start_load_proc(rows_per_sec)
        except Exception as exc:  # pylint: disable=broad-except
            subprocess.run(
                ["docker", "compose", "-f", str(compose_file), "down", "--remove-orphans"],
                env=env,
            )
            return {
                "query": cfg.name,
                "tcp_query_file": str(tcp_query),
                "error": f"failed_to_start_load_generator:{exc}",
            }
        time.sleep(run_seconds)
        proc.terminate()
        try:
            proc.wait(timeout=10.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5.0)

    load_end = time.monotonic()
    effective_run_seconds = load_end - load_start

    # Allow some time for windows/sinks to flush.
    if flush_seconds > 0:
        time.sleep(flush_seconds)

    # Stop the stack.
    subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "down", "--remove-orphans"],
        env=env,
    )

    result: Dict[str, Any] = {
        "query": cfg.name,
        "tcp_query_file": str(tcp_query),
        "output_csv": str(cfg.output_csv),
        "run_seconds_wall": effective_run_seconds,
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
