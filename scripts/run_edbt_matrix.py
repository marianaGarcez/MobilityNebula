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
import csv
import json
import os
import re
import socket
import subprocess
import sys
import threading
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


def _tgeo_metrics_path(output_csv: Path) -> Path:
    return output_csv.with_name("tgeo_at_stbox_metrics.json")


def _geo_metrics_path(output_csv: Path) -> Path:
    return output_csv.with_name("geo_function_metrics.json")


def _geo_operator_metrics_path(output_csv: Path) -> Path:
    return output_csv.with_name("geo_operator_metrics.json")


def _clear_file(path: Path) -> None:
    try:
        if path.exists():
            path.unlink()
    except Exception:
        pass


def _read_json_file(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _env_float(keys: List[str]) -> Optional[float]:
    for key in keys:
        raw = os.environ.get(key)
        if raw is None:
            continue
        raw = raw.strip()
        if not raw:
            continue
        try:
            return float(raw)
        except ValueError:
            continue
    return None


def _window_override_ms() -> tuple[Optional[float], Optional[float]]:
    size_ms = _env_float(["WINDOW_SIZE_MS", "WINDOW_SIZE_MILLISECONDS"])
    if size_ms is None:
        size_sec = _env_float(["WINDOW_SIZE_SEC", "WINDOW_SIZE_SECONDS"])
        if size_sec is not None:
            size_ms = size_sec * 1000.0

    slide_ms = _env_float(["WINDOW_SLIDE_MS", "WINDOW_ADVANCE_MS"])
    if slide_ms is None:
        slide_sec = _env_float(["WINDOW_SLIDE_SEC", "WINDOW_ADVANCE_SEC"])
        if slide_sec is not None:
            slide_ms = slide_sec * 1000.0

    return size_ms, slide_ms


def _window_override_type() -> Optional[str]:
    raw = os.environ.get("WINDOW_TYPE_OVERRIDE") or os.environ.get("WINDOW_TYPE")
    if raw is None:
        return None
    val = raw.strip().lower()
    if val in {"tumbling", "sliding"}:
        return val
    return None


def _format_slide_clause(slide_ms: float) -> tuple[str, str]:
    slide_int = int(round(slide_ms))
    if slide_int >= 1000 and (slide_int % 1000 == 0):
        return str(slide_int // 1000), "SEC"
    return str(slide_int), "MS"


def _apply_window_overrides(
    query_path: Path,
    size_ms: Optional[float],
    slide_ms: Optional[float],
    window_type_override: Optional[str],
) -> Path:
    if size_ms is None and slide_ms is None and not window_type_override:
        return query_path

    text = query_path.read_text(encoding="utf-8")
    original = text
    size_sec = None
    if size_ms is not None:
        size_sec = int(round(size_ms / 1000.0))

    if window_type_override == "tumbling":
        text = re.sub(
            r"WINDOW\s+SLIDING\s*\(\s*([^,]+),\s*SIZE\s+(\d+(?:\.\d+)?)\s+SEC\s*,\s*ADVANCE\s+BY\s+\d+(?:\.\d+)?\s+(?:MS|SEC)\s*\)",
            r"WINDOW TUMBLING(\1, SIZE \2 SEC)",
            text,
            flags=re.IGNORECASE,
        )
    elif window_type_override == "sliding":
        if slide_ms is None:
            slide_ms = size_ms
        if slide_ms is not None:
            slide_val, slide_unit = _format_slide_clause(slide_ms)
            text = re.sub(
                r"WINDOW\s+TUMBLING\s*\(\s*([^,]+),\s*SIZE\s+(\d+(?:\.\d+)?)\s+SEC\s*\)",
                rf"WINDOW SLIDING(\1, SIZE \2 SEC, ADVANCE BY {slide_val} {slide_unit})",
                text,
                flags=re.IGNORECASE,
            )

    if "WINDOW SLIDING" in text.upper() and size_sec is not None:
        text = re.sub(
            r"(WINDOW\s+SLIDING\s*\(\s*[^,]+,\s*SIZE\s+)"
            r"(\d+(?:\.\d+)?)\s+SEC",
            rf"\g<1>{size_sec} SEC",
            text,
            flags=re.IGNORECASE,
        )
        if slide_ms is not None:
            slide_val, slide_unit = _format_slide_clause(slide_ms)
            text = re.sub(
                r"(ADVANCE\s+BY\s+)(\d+(?:\.\d+)?)(\s+)(MS|SEC)",
                rf"\g<1>{slide_val} {slide_unit}",
                text,
                flags=re.IGNORECASE,
            )

    if "WINDOW TUMBLING" in text.upper() and size_sec is not None:
        text = re.sub(
            r"(WINDOW\s+TUMBLING\s*\(\s*[^,]+,\s*SIZE\s+)"
            r"(\d+(?:\.\d+)?)\s+SEC",
            rf"\g<1>{size_sec} SEC",
            text,
            flags=re.IGNORECASE,
        )

    if text == original:
        return query_path

    suffix = query_path.suffix or ".yaml"
    name_bits = [query_path.stem]
    if size_sec is not None:
        name_bits.append(f"win{size_sec}s")
    if slide_ms is not None:
        slide_val, slide_unit = _format_slide_clause(slide_ms)
        name_bits.append(f"slide{slide_val}{slide_unit.lower()}")
    out_name = "_".join(name_bits) + suffix
    out_dir = query_path.parent / "_window_overrides"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / out_name
    out_path.write_text(text, encoding="utf-8")
    return out_path


def build_default_query_matrix(repo_root: Path) -> List[QueryConfig]:
    def env_truthy(key: str) -> bool:
        raw = os.environ.get(key, "")
        return raw.strip().lower() in {"1", "true", "yes", "on"}

    use_tgeo_q4 = env_truthy("NES_Q4_TGEO")

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
        query_file=repo_root / "Queries" / ("Query4-tgeo-csv.yaml" if use_tgeo_q4 else "Query4-csv.yaml"),
        output_csv=repo_root / "Output" / "output_query4.csv",
        start_col=0,
        end_col=1,
        tcp_query_file=repo_root / "Queries" / ("Query4-tgeo.yaml" if use_tgeo_q4 else "Query4.yaml"),
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
    compose_overrides: Optional[List[Path]] = None,
    compose_compatibility: bool = False,
) -> Dict[str, Any]:
    """
    Run a single query via docker-compose.runtime.yaml and collect metrics.
    """
    if not cfg.query_file.is_file():
        return {
            "query": cfg.name,
            "error": f"query_file_not_found:{cfg.query_file}",
        }

    override_size_ms, override_slide_ms = _window_override_ms()
    override_type = _window_override_type()
    query_file = _apply_window_overrides(cfg.query_file, override_size_ms, override_slide_ms, override_type)
    if not query_file.is_file():
        return {
            "query": cfg.name,
            "error": f"query_file_not_found:{query_file}",
        }

    # Ensure output directory exists and previous file does not interfere.
    cfg.output_csv.parent.mkdir(parents=True, exist_ok=True)
    _clear_file(cfg.output_csv)
    try:
        # Touch the sink file so "output_csv_not_found" reflects true failures,
        # not just queries that never emit any rows.
        cfg.output_csv.touch(exist_ok=True)
    except Exception:
        pass
    geo_metrics_path = _geo_metrics_path(cfg.output_csv)
    _clear_file(geo_metrics_path)
    geo_operator_metrics_path = _geo_operator_metrics_path(cfg.output_csv)
    _clear_file(geo_operator_metrics_path)
    tgeo_metrics_path = _tgeo_metrics_path(cfg.output_csv)
    _clear_file(tgeo_metrics_path)

    env = os.environ.copy()
    queries_dir = compose_file.parent / "Queries"
    try:
        rel = query_file.relative_to(queries_dir)
        query_path = f"/workspace/Queries/{rel.as_posix()}"
    except ValueError:
        query_path = f"/workspace/Queries/{query_file.name}"
    env["NES_QUERY_FILE"] = query_path
    env["NES_WORKER_THREADS"] = str(worker_threads)
    if runtime_image:
        env["NES_RUNTIME_IMAGE"] = runtime_image

    compose_files = [compose_file] + (compose_overrides or [])
    compose_base = ["docker", "compose"]
    if compose_compatibility:
        compose_base.append("--compatibility")
    for f in compose_files:
        compose_base.extend(["-f", str(f)])

    cmd = compose_base + [
        "up",
        "--force-recreate",
        "--abort-on-container-exit",
    ]

    start_time = time.monotonic()
    # Capture docker-compose output so callers that parse stdout as JSON (e.g., edbt_bench)
    # don't get their output polluted by compose logs.
    completed = subprocess.run(
        cmd,
        env=env,
        cwd=str(compose_file.parent),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    end_time = time.monotonic()
    run_seconds = end_time - start_time

    result: Dict[str, Any] = {
        "query": cfg.name,
        "query_file": str(query_file),
        "output_csv": str(cfg.output_csv),
        "run_seconds_wall": run_seconds,
        "docker_returncode": completed.returncode,
    }

    try:
        stdout_text = (completed.stdout or b"").decode("utf-8", errors="replace")
        stderr_text = (completed.stderr or b"").decode("utf-8", errors="replace")
        if stdout_text.strip():
            result["docker_compose_stdout_tail"] = "\n".join(stdout_text.splitlines()[-200:])
        if stderr_text.strip():
            result["docker_compose_stderr_tail"] = "\n".join(stderr_text.splitlines()[-200:])
    except Exception:
        pass

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
    geo_metrics = _read_json_file(geo_metrics_path)
    if geo_metrics:
        result["geo_function_metrics"] = geo_metrics
    geo_operator_metrics = _read_json_file(geo_operator_metrics_path)
    if geo_operator_metrics:
        result["geo_operator_metrics"] = geo_operator_metrics
    tgeo_metrics = _read_json_file(tgeo_metrics_path)
    if tgeo_metrics:
        result["tgeo_at_stbox_metrics"] = tgeo_metrics
    elif isinstance(geo_metrics, dict):
        tgeo_from_geo = geo_metrics.get("tgeo_at_stbox")
        if isinstance(tgeo_from_geo, dict) and tgeo_from_geo:
            result["tgeo_at_stbox_metrics"] = tgeo_from_geo
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
    warmup_seconds: float = 0.0,
    measure_seconds: float | None = None,
    emit_unit: str = "ms",
    # Default to Karimov-style event-time: generated by the driver at a constant rate.
    # Avoid 'shift' by default because accelerated replay can place event-time ahead of wall-clock,
    # producing negative latencies (which get clamped to 0ms in the summary).
    rewrite_ts: str = "rate",
    ts_output_unit: str = "ms",
    compose_overrides: Optional[List[Path]] = None,
    compose_compatibility: bool = False,
    docker_stats_interval_seconds: float = 0.0,
    net_seed: int | None = None,
    net_jitter_ms: float = 0.0,
    net_burst_on_s: float = 0.0,
    net_burst_off_s: float = 0.0,
    net_outage_every_s: float = 0.0,
    net_outage_duration_s: float = 0.0,
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

    override_size_ms, override_slide_ms = _window_override_ms()
    override_type = _window_override_type()
    tcp_query = _apply_window_overrides(tcp_query, override_size_ms, override_slide_ms, override_type)
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

    compose_files = [compose_file] + (compose_overrides or [])
    compose_base = ["docker", "compose"]
    if compose_compatibility:
        compose_base.append("--compatibility")
    for f in compose_files:
        compose_base.extend(["-f", str(f)])

    def docker_compose_service_id(service: str) -> str:
        try:
            out = subprocess.check_output(
                compose_base + ["ps", "--all", "-q", service],
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

    def _parse_size_to_mib(s: str) -> Optional[float]:
        s = s.strip()
        m = re.match(r"^([0-9]*\\.?[0-9]+)\\s*([KMG]i?B|B)$", s, flags=re.IGNORECASE)
        if not m:
            return None
        val = float(m.group(1))
        unit = m.group(2).lower()
        if unit == "b":
            return val / (1024.0 * 1024.0)
        if unit in ("kib", "kb"):
            return val / 1024.0
        if unit in ("mib", "mb"):
            return val
        if unit in ("gib", "gb"):
            return val * 1024.0
        return None

    docker_stats_last_error: str | None = None
    cgroup_stats_last_error: str | None = None

    def _sample_docker_stats(container: str) -> Optional[Dict[str, float]]:
        nonlocal docker_stats_last_error
        try:
            out = subprocess.check_output(
                [
                    "docker",
                    "stats",
                    "--no-stream",
                    "--format",
                    "{{.CPUPerc}},{{.MemUsage}}",
                    container,
                ],
                stderr=subprocess.STDOUT,
            ).decode("utf-8", errors="replace").strip()
            if not out:
                return None
            cpu_s, mem_s = out.split(",", 1)
            cpu = float(cpu_s.strip().rstrip("%"))
            mem_used = mem_s.split("/", 1)[0].strip()
            mem_mib = _parse_size_to_mib(mem_used)
            if mem_mib is None:
                return None
            return {"cpu_percent": cpu, "mem_mib": float(mem_mib)}
        except subprocess.CalledProcessError as exc:
            docker_stats_last_error = exc.output.decode("utf-8", errors="replace") if getattr(exc, "output", None) else str(exc)
            return None
        except Exception:
            return None

    def _sample_cgroup_stats(container: str, cpus_limit: float, prev: dict[str, float]) -> Optional[Dict[str, float]]:
        """
        Fallback sampler when `docker stats` is unavailable.

        Reads cgroup counters from inside the container and derives CPU% from
        delta usage over delta wall time.

        Supports:
          - cgroup v2:
              * /sys/fs/cgroup/memory.current (bytes)
              * /sys/fs/cgroup/cpu.stat usage_usec
          - cgroup v1:
              * /sys/fs/cgroup/memory/memory.usage_in_bytes (bytes)
              * /sys/fs/cgroup/cpuacct/cpuacct.usage (nanoseconds)
        """
        nonlocal cgroup_stats_last_error
        try:
            # Read memory + cpu counters in a single exec to reduce overhead.
            out = subprocess.check_output(
                [
                    "docker",
                    "exec",
                    container,
                    "sh",
                    "-lc",
                    r"""
set -eu

# Memory (bytes). Be permissive and never fail the script due to missing files.
if [ -r /sys/fs/cgroup/memory.current ]; then
  cat /sys/fs/cgroup/memory.current 2>/dev/null || true
elif [ -r /sys/fs/cgroup/memory/memory.usage_in_bytes ]; then
  cat /sys/fs/cgroup/memory/memory.usage_in_bytes 2>/dev/null || true
elif [ -r /sys/fs/cgroup/memory.usage_in_bytes ]; then
  cat /sys/fs/cgroup/memory.usage_in_bytes 2>/dev/null || true
else
  echo ""
fi

# CPU usage + unit (avoid awk/grep dependencies; use shell built-ins).
if [ -r /sys/fs/cgroup/cpu.stat ]; then
  usage=""
  while read -r k v; do
    if [ "$k" = "usage_usec" ]; then
      usage="$v"
      break
    fi
  done < /sys/fs/cgroup/cpu.stat
  echo "usec ${usage}"
elif [ -r /sys/fs/cgroup/cpuacct/cpuacct.usage ]; then
  usage="$(cat /sys/fs/cgroup/cpuacct/cpuacct.usage 2>/dev/null || true)"
  echo "nsec ${usage}"
elif [ -r /sys/fs/cgroup/cpuacct.usage ]; then
  usage="$(cat /sys/fs/cgroup/cpuacct.usage 2>/dev/null || true)"
  echo "nsec ${usage}"
else
  echo ""
fi
""".strip(),
                ],
                stderr=subprocess.STDOUT,
            ).decode("utf-8", errors="replace")
        except subprocess.CalledProcessError as exc:
            cgroup_stats_last_error = exc.output.decode("utf-8", errors="replace") if getattr(exc, "output", None) else str(exc)
            return None
        except Exception as exc:
            cgroup_stats_last_error = f"{type(exc).__name__}:{exc}"
            return None

        lines = [l.strip() for l in out.splitlines() if l.strip()]
        if len(lines) < 2:
            return None
        try:
            mem_bytes = float(lines[0])
        except Exception:
            mem_bytes = 0.0

        cpu_usage = None
        cpu_unit = None
        try:
            parts = lines[1].split()
            if len(parts) >= 2:
                cpu_unit = parts[0].strip()
                cpu_usage = float(parts[1])
        except Exception:
            cpu_unit = None
            cpu_usage = None
        if cpu_usage is None or cpu_unit not in ("usec", "nsec"):
            return None

        now = time.monotonic()
        prev_usage = float(prev.get("usage", cpu_usage))
        prev_t = float(prev.get("t", now))
        prev["usage"] = cpu_usage
        prev["unit"] = cpu_unit
        prev["t"] = now

        dt = max(1e-6, now - prev_t)
        du = max(0.0, cpu_usage - prev_usage)
        # Convert usage delta to seconds.
        if cpu_unit == "usec":
            cpu_seconds = du / 1_000_000.0
        else:  # nsec
            cpu_seconds = du / 1_000_000_000.0
        # CPU% = (cpu_time / wall_time) / cpus_limit * 100
        cpu_pct = cpu_seconds / dt
        if cpus_limit > 0:
            cpu_pct = (cpu_pct / cpus_limit) * 100.0
        else:
            cpu_pct = cpu_pct * 100.0

        return {
            "cpu_percent": float(cpu_pct),
            "mem_mib": float(mem_bytes / (1024.0 * 1024.0)),
        }

    # Ensure output directory exists and previous file does not interfere.
    cfg.output_csv.parent.mkdir(parents=True, exist_ok=True)
    _clear_file(cfg.output_csv)
    try:
        # Touch the sink file so "output_csv_not_found" reflects true failures,
        # not just runs with zero sink output.
        cfg.output_csv.touch(exist_ok=True)
    except Exception:
        pass
    geo_metrics_path = _geo_metrics_path(cfg.output_csv)
    _clear_file(geo_metrics_path)
    geo_operator_metrics_path = _geo_operator_metrics_path(cfg.output_csv)
    _clear_file(geo_operator_metrics_path)
    tgeo_metrics_path = _tgeo_metrics_path(cfg.output_csv)
    _clear_file(tgeo_metrics_path)

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
    # Use a per-run tag so stale/background drivers cannot corrupt log parsing.
    run_tag = f"p{chosen_port}_{int(time.time() * 1000)}"
    source_log = run_log_dir / f"{cfg.name}_tcp_source_{run_tag}.log"
    source_log.write_text("", encoding="utf-8")
    source_stats = run_log_dir / f"{cfg.name}_tcp_source_stats_{run_tag}.json"
    start_file = run_log_dir / f"{cfg.name}_tcp_start_{run_tag}.signal"

    # Record wall-clock emission timestamps for end-to-end event-time latency.
    emit_dir_raw = os.environ.get("EMIT_SCRATCH_DIR", "").strip()
    emit_dir: Path | None = None
    if emit_dir_raw:
        try:
            emit_dir = Path(emit_dir_raw).expanduser()
            emit_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            emit_dir = None
    if emit_dir:
        emit_csv = emit_dir / f"{cfg.name}_emit_{run_tag}{cfg.output_csv.suffix}"
    else:
        emit_csv = cfg.output_csv.with_name(f"{cfg.output_csv.stem}_emit{cfg.output_csv.suffix}")
    if emit_csv.exists():
        emit_csv.unlink()
    recorder_script = Path(__file__).resolve().parent / "file_sink_timestamp_recorder.py"
    recorder_log = run_log_dir / f"{cfg.name}_sink_recorder_{run_tag}.log"
    recorder_log.write_text("", encoding="utf-8")
    recorder_proc: subprocess.Popen | None = None

    # For multi-source TCP queries (joins), both TCP sources must observe the
    # same event-time sequence. Use the driver's --broadcast mode so timestamps
    # are generated once and sent to all connected clients.
    expected_clients = 2 if cfg.name in {"Q6", "Q7", "Q9"} else 1
    broadcast_driver = expected_clients > 1

    override_window_seconds = (override_size_ms / 1000.0) if override_size_ms is not None else None

    def window_size_seconds_for_query(query_name: str) -> float:
        # Used only for the driver-side processing-time latency proxy (maps window_end -> last send time).
        # Keep aligned with the MobilityNebula query definitions.
        if override_window_seconds is not None and override_window_seconds > 0:
            return float(override_window_seconds)
        return {
            "Q1": 10.0,
            "Q2": 10.0,
            "Q3": 10.0,
            "Q4": 10.0,
            "Q5": 45.0,
            "Q6": 10.0,
            "Q7": 10.0,
            "Q8": 10.0,
            "Q9": 10.0,
        }.get(query_name, 10.0)

    def start_load_proc(rps_total: float) -> subprocess.Popen:
        rps = (rps_total / expected_clients) if broadcast_driver and expected_clients > 0 else rps_total
        # Prevent extremely bursty arrivals at low rates:
        # The driver sleeps once per batch, so a large batch at low rps can create long idle
        # gaps (and huge bursts) that destabilize the TCP source and windowed operators.
        #
        # Cap the batch interval to keep arrivals reasonably smooth. Override/disable via
        # TCP_BATCH_INTERVAL_MAX_S (set to 0 to disable).
        effective_batch_size = int(batch_size)
        # NOTE: Use a slightly larger default so low-rate multi-source runs (e.g., Q9 at 50 eps,
        # broadcasted to 2 TCP sources) do not degenerate to batch_size=1 (which increases syscall
        # overhead and has proven less stable over long runs). Override/disable via
        # TCP_BATCH_INTERVAL_MAX_S (set to 0 to disable).
        try:
            max_batch_interval_s = float(os.environ.get("TCP_BATCH_INTERVAL_MAX_S", "0.1") or 0.1)
        except Exception:
            max_batch_interval_s = 0.1
        if max_batch_interval_s > 0.0 and rps > 0.0:
            max_batch = max(1, int(rps * max_batch_interval_s))
            effective_batch_size = min(effective_batch_size, max_batch)
        # Auto-detect a CSV header line (e.g., "time_utc,device_id,...") so we
        # can use the shared dataset with header across systems.
        skip_header = False
        try:
            with open(input_csv, "r", encoding="utf-8", errors="replace") as f_in:
                first = f_in.readline().strip()
            if first and any(ch.isalpha() for ch in first):
                skip_header = True
        except Exception:
            skip_header = False

        order_scope = os.environ.get("TCP_ORDER_SCOPE", "per-key").strip() or "per-key"
        key_col_raw = os.environ.get("TCP_KEY_COL_INDEX")
        if key_col_raw is None:
            key_cols = ["1"]
        else:
            key_col_raw = key_col_raw.strip()
            key_cols = [c for c in re.split(r"[,\s]+", key_col_raw) if c] if key_col_raw else []
        no_order = os.environ.get("TCP_NO_ORDER", "").strip().lower() in {"1", "true", "yes", "on"}
        skip_rows_raw = os.environ.get("TCP_SKIP_ROWS", "").strip()
        skip_rows = None
        if skip_rows_raw:
            try:
                skip_rows = max(0, int(skip_rows_raw))
            except ValueError:
                skip_rows = None

        cmd = [
            sys.executable,
            "-u",
            str(server_script),
            str(input_csv),
            "--host",
            host,
            "--port",
            str(chosen_port),
        ]
        # Reduce packetization latency for small batches (helps at low rates / under jitter).
        cmd.append("--tcp-nodelay")
        if skip_header:
            cmd.append("--skip-header")
        if skip_rows is not None:
            cmd += ["--skip-rows", str(skip_rows)]
        cmd += [
            "--rows-per-sec",
            str(rps),
            "--batch-size",
            str(effective_batch_size),
            "--loop",
            "--log-connections",
            "--start-file",
            str(start_file),
            "--warmup-seconds",
            str(float(warmup_seconds)),
            "--ts-output-unit",
            ts_output_unit,
            "--rewrite-ts",
            ("rate_grouped" if (broadcast_driver and rewrite_ts == "rate") else rewrite_ts),
            "--rewrite-monotonic-step",
            "0",
            "--window-size-s",
            str(window_size_seconds_for_query(cfg.name)),
            "--stats-out",
            str(source_stats),
        ]
        if no_order:
            cmd.append("--no-order")
        else:
            # Ensure timestamps keep increasing even when looping and across duplicates.
            # Also avoid dropping events from different devices that share the same timestamp.
            cmd += ["--order-scope", order_scope]
            if order_scope in ("per-key", "both"):
                if not key_cols:
                    key_cols = ["1"]
                cmd += ["--key-col-index", *key_cols]
        if net_seed is not None:
            cmd += ["--seed", str(int(net_seed))]
        if net_jitter_ms and float(net_jitter_ms) > 0.0:
            cmd += ["--jitter-ms", str(float(net_jitter_ms))]
        if net_burst_on_s and net_burst_off_s and float(net_burst_on_s) > 0.0 and float(net_burst_off_s) > 0.0:
            cmd += ["--burst-on-s", str(float(net_burst_on_s)), "--burst-off-s", str(float(net_burst_off_s))]
        if net_outage_every_s and float(net_outage_every_s) > 0.0:
            cmd += ["--outage-every-s", str(float(net_outage_every_s))]
            if net_outage_duration_s and float(net_outage_duration_s) > 0.0:
                cmd += ["--outage-duration-s", str(float(net_outage_duration_s))]
        if broadcast_driver:
            # In broadcast mode we must wait for all expected clients before we
            # start the timed benchmark section; otherwise the driver may spend
            # most/all of the run waiting for the second TCP source to connect,
            # yielding zero sink output (observed for Q9).
            #
            # Keep this comfortably above the typical query startup time so both
            # sources can connect, but bounded so the run can fail fast if a
            # source never connects.
            accept_timeout = float(os.environ.get("TCP_ACCEPT_TIMEOUT", "45") or 45.0)
            cmd += [
                "--broadcast",
                "--expected-clients",
                str(expected_clients),
                "--accept-timeout",
                str(accept_timeout),
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
    up_cmd = compose_base + ["up", "-d", "--force-recreate"]
    queries_dir = compose_file.parent / "Queries"
    try:
        rel = tcp_query_materialized.relative_to(queries_dir)
        env["NES_QUERY_FILE"] = f"/workspace/Queries/{rel.as_posix()}"
    except ValueError:
        env["NES_QUERY_FILE"] = f"/workspace/Queries/{tcp_query_materialized.name}"
    # Guard against stale containers with fixed names (e.g., nes-worker/mosquitto)
    # that can cause docker compose to fail with "container name already in use".
    try:
        pre_clean = str(os.environ.get("COMPOSE_PRE_CLEAN", "1")).strip().lower()
        if pre_clean not in {"0", "false", "no"}:
            subprocess.run(
                compose_base + ["down", "--remove-orphans"],
                env=env,
                cwd=str(compose_file.parent),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
    except Exception:
        pass
    up_res = subprocess.run(
        up_cmd,
        env=env,
        cwd=str(compose_file.parent),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
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
            "docker_compose_stdout": up_res.stdout.decode("utf-8", errors="replace"),
            "docker_compose_stderr": up_res.stderr.decode("utf-8", errors="replace"),
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
            compose_base + ["down", "--remove-orphans"],
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
            compose_base + ["down", "--remove-orphans"],
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

    # Sanity check: ensure the worker(s) actually connected to the TCP source.
    # For broadcast (multi-source) queries, we require all expected clients to
    # connect; otherwise the driver can end up streaming to only one source and
    # the query will legitimately produce no output.
    required_clients = expected_clients if broadcast_driver else 1
    connected_deadline = time.time() + float(os.environ.get("TCP_CONNECT_TIMEOUT", "20") or 20.0)
    connected = 0
    while time.time() < connected_deadline:
        try:
            log_txt = source_log.read_text(encoding="utf-8", errors="replace")
            connected = log_txt.count("Client connected from")
            if connected >= required_clients:
                break
        except Exception:
            pass
        time.sleep(0.25)
    else:
        subprocess.run(
            compose_base + ["down", "--remove-orphans"],
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
            "error": "tcp_source_never_connected" if connected == 0 else "tcp_source_missing_clients",
            "tcp_source_log": str(source_log),
            "tcp_port": chosen_port,
            "expected_clients": required_clients,
            "connected_clients": connected,
        }

    # Start the sink timestamp recorder once we know the query is registered.
    try:
        rec_cmd = [
            sys.executable,
            "-u",
            str(recorder_script),
            "--input",
            str(cfg.output_csv),
            "--out",
            str(emit_csv),
            "--emit-unit",
            emit_unit,
        ]
        f = recorder_log.open("a", encoding="utf-8")
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        recorder_proc = subprocess.Popen(rec_cmd, stdout=f, stderr=f, env=env)
        f.close()
    except Exception:
        recorder_proc = None

    # Signal the driver to start streaming only after:
    #  1) the query is registered, 2) all expected TCP sources have connected,
    #  3) the sink recorder is running (so we don't miss early output rows).
    try:
        start_file.write_text("start\n", encoding="utf-8")
    except Exception as exc:
        subprocess.run(
            compose_base + ["down", "--remove-orphans"],
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
        if recorder_proc is not None:
            recorder_proc.terminate()
            try:
                recorder_proc.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                recorder_proc.kill()
                recorder_proc.wait(timeout=5.0)
        return {
            "query": cfg.name,
            "tcp_query_file": str(tcp_query_materialized),
            "error": f"failed_to_create_start_file:{type(exc).__name__}:{exc}",
            "start_file": str(start_file),
        }

    load_start = time.monotonic()

    # Optional docker stats sampling for cpu/memory peaks.
    docker_samples: list[dict[str, float]] = []
    docker_stop = threading.Event()

    # Resolve a stable identifier for the worker container for docker stats.
    # Prefer explicit container name, but fall back to the compose-resolved ID.
    worker_container = "nes-worker"
    try:
        worker_id = docker_compose_service_id("nes-worker")
        if worker_id:
            worker_container = worker_id
    except Exception:
        worker_container = "nes-worker"
    try:
        cpus_limit = float(os.environ.get("NES_CPUS", "0") or 0.0)
    except Exception:
        cpus_limit = 0.0
    cgroup_prev: dict[str, float] = {}

    def docker_sampler() -> None:
        while not docker_stop.is_set():
            # Try both the stable name and the resolved ID; Docker differs across
            # platforms/compose configs.
            s = _sample_docker_stats("nes-worker") or _sample_docker_stats(worker_container)
            if s is None:
                # Fallback: read cgroup counters from inside the container.
                s = _sample_cgroup_stats(worker_container, cpus_limit, cgroup_prev) or _sample_cgroup_stats("nes-worker", cpus_limit, cgroup_prev)
            if s is not None:
                docker_samples.append(s)
            time.sleep(max(0.05, docker_stats_interval_seconds))

    docker_thread: threading.Thread | None = None
    if docker_stats_interval_seconds and docker_stats_interval_seconds > 0:
        docker_thread = threading.Thread(target=docker_sampler, daemon=True)
        docker_thread.start()

    # Wait for the configured duration, but fail fast if the TCP driver exits early
    # (typically because the SUT closed the connection due to a crash/restart).
    driver_exited_early = False
    driver_returncode: int | None = None
    deadline = time.monotonic() + float(run_seconds)
    while time.monotonic() < deadline:
        if load_proc is not None:
            rc = load_proc.poll()
            if rc is not None:
                driver_exited_early = True
                driver_returncode = int(rc)
                break
        time.sleep(0.25)
    if load_proc is not None:
        if load_proc.poll() is None:
            load_proc.terminate()
            try:
                load_proc.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                load_proc.kill()
                load_proc.wait(timeout=5.0)

    load_end = time.monotonic()
    effective_run_seconds = load_end - load_start

    # Allow some time for windows/sinks to flush.
    try:
        flush_seconds = max(float(flush_seconds), float(window_size_seconds_for_query(cfg.name)))
    except Exception:
        flush_seconds = float(flush_seconds)
    if flush_seconds > 0:
        time.sleep(float(flush_seconds))

    # Optional extra drain when the sink has not produced any rows yet (late emission).
    # This is useful for low-rate, join-heavy queries like Q9 where windows may close
    # after the driver stops. Controlled via SINK_EXTRA_DRAIN_S (default: 0).
    try:
        extra_drain_s = float(os.environ.get("SINK_EXTRA_DRAIN_S", "0") or 0.0)
    except Exception:
        extra_drain_s = 0.0
    if extra_drain_s > 0:
        try:
            has_rows = False
            if cfg.output_csv.is_file():
                with cfg.output_csv.open("r", encoding="utf-8", newline="") as f:
                    reader = csv.reader(f)
                    for row in reader:
                        if not row:
                            continue
                        if cfg.start_col >= len(row) or cfg.end_col >= len(row):
                            continue
                        try:
                            int(float(row[cfg.start_col]))
                            int(float(row[cfg.end_col]))
                        except Exception:
                            continue
                        has_rows = True
                        break
            if not has_rows:
                time.sleep(float(extra_drain_s))
        except Exception:
            pass

    docker_stop.set()
    if docker_thread is not None:
        docker_thread.join(timeout=2.0)

    # Capture worker state before teardown.
    worker_state: Dict[str, Any] = {}
    worker_container_id: str = ""
    try:
        worker_container_id = docker_compose_service_id("nes-worker") or ""
    except Exception:
        worker_container_id = ""

    def _inspect_worker(name_or_id: str) -> Dict[str, Any]:
        if not name_or_id:
            return {}
        try:
            raw = subprocess.check_output(
                [
                    "docker",
                    "inspect",
                    "--format",
                    '{"State": {{json .State}}, "RestartCount": {{json .RestartCount}}}',
                    name_or_id,
                ],
                stderr=subprocess.STDOUT,
            ).decode("utf-8", errors="replace").strip()
            obj = json.loads(raw) if raw else {}
            state = obj.get("State") if isinstance(obj, dict) else {}
            rc = obj.get("RestartCount") if isinstance(obj, dict) else None
            out: Dict[str, Any] = {}
            if isinstance(state, dict):
                out["status"] = state.get("Status")
                out["oom_killed"] = bool(state.get("OOMKilled") is True)
                out["exit_code"] = state.get("ExitCode")
                out["error"] = state.get("Error")
                out["started_at"] = state.get("StartedAt")
                out["finished_at"] = state.get("FinishedAt")
            if rc is not None:
                out["restart_count"] = rc
            return out
        except Exception:
            return {}

    # Prefer a stable name, but fall back to the compose-resolved ID.
    worker_state = _inspect_worker("nes-worker") or _inspect_worker(worker_container_id)
    if worker_container_id and worker_state:
        worker_state["container_id"] = worker_container_id

    early_disconnect_logs: Dict[str, str] = {}
    if driver_exited_early:
        try:
            worker_id = docker_compose_service_id("nes-worker")
            if worker_id:
                early_disconnect_logs["nes_worker_logs_tail"] = docker_logs(worker_id)
        except Exception:
            pass
        try:
            qr_id = docker_compose_service_id("query-registration")
            if qr_id:
                early_disconnect_logs["query_registration_logs_tail"] = docker_logs(qr_id)
        except Exception:
            pass

    # If we produced no sink output (header-only/empty file), capture logs before teardown.
    # This helps debug queries that "timeout" (no rows) without an explicit runtime error.
    empty_output_logs: Dict[str, str] = {}
    no_sink_rows = False
    if (not driver_exited_early) and cfg.output_csv.is_file():
        try:
            with cfg.output_csv.open("r", encoding="utf-8", newline="") as f:
                reader = csv.reader(f)
                for row in reader:
                    if not row:
                        continue
                    if cfg.start_col >= len(row) or cfg.end_col >= len(row):
                        continue
                    try:
                        int(float(row[cfg.start_col]))
                        int(float(row[cfg.end_col]))
                    except Exception:
                        continue
                    no_sink_rows = False
                    break
                else:
                    no_sink_rows = True
        except Exception:
            no_sink_rows = False

    if no_sink_rows:
        try:
            worker_id = docker_compose_service_id("nes-worker") or worker_container_id
            if worker_id:
                empty_output_logs["nes_worker_logs_tail"] = docker_logs(worker_id)
        except Exception:
            pass
        try:
            if query_reg_id:
                empty_output_logs["query_registration_logs_tail"] = docker_logs(query_reg_id)
        except Exception:
            pass

    # If the sink file is missing before teardown, capture logs now because
    # docker compose down will remove the containers.
    missing_output_logs: Dict[str, str] = {}
    missing_output_before_down = not cfg.output_csv.is_file()
    if missing_output_before_down:
        try:
            worker_id = docker_compose_service_id("nes-worker") or worker_container_id
            if worker_id:
                missing_output_logs["nes_worker_logs_tail"] = docker_logs(worker_id)
        except Exception:
            pass
        try:
            if query_reg_id:
                missing_output_logs["query_registration_logs_tail"] = docker_logs(query_reg_id)
        except Exception:
            pass

    # Stop the stack.
    subprocess.run(
        compose_base + ["down", "--remove-orphans"],
        env=env,
        cwd=str(compose_file.parent),
    )
    try:
        if start_file.exists():
            start_file.unlink()
    except Exception:
        pass

    # Keep the sink timestamp recorder running through teardown, because the
    # File sink may flush buffered rows during shutdown (notably for join-heavy
    # queries like Q9). Terminate it only after the stack is down and the sink
    # file has had a chance to settle.
    def _wait_file_stable(path: Path, timeout_s: float = 5.0, stable_for_s: float = 0.3) -> None:
        if not path:
            return
        deadline = time.monotonic() + max(0.0, timeout_s)
        last_size: int | None = None
        stable_since = time.monotonic()
        while time.monotonic() < deadline:
            try:
                size = int(path.stat().st_size)
            except Exception:
                size = -1
            if last_size is None or size != last_size:
                last_size = size
                stable_since = time.monotonic()
            elif (time.monotonic() - stable_since) >= stable_for_s:
                return
            time.sleep(0.05)

    _wait_file_stable(cfg.output_csv)
    _wait_file_stable(emit_csv)
    if recorder_proc is not None:
        recorder_proc.terminate()
        try:
            recorder_proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            recorder_proc.kill()
            recorder_proc.wait(timeout=5.0)

    result: Dict[str, Any] = {
        "query": cfg.name,
        "tcp_query_file": str(tcp_query_materialized),
        "output_csv": str(cfg.output_csv),
        "emit_csv": str(emit_csv),
        "run_seconds_wall": effective_run_seconds,
        "tcp_source_log": str(source_log),
        "tcp_source_stats": str(source_stats),
        "sink_recorder_log": str(recorder_log),
        "tcp_port": chosen_port,
    }
    if driver_exited_early:
        # The TCP driver terminated before the configured run duration because one or more
        # clients disconnected (or the driver crashed). Classify as an error unless we
        # already collected (almost) the full measurement interval.
        result["tcp_source_disconnected_after_s"] = float(effective_run_seconds)
        result["tcp_source_disconnected_returncode"] = int(driver_returncode or 0)

        disconnect_is_late = False
        if measure_seconds is not None:
            try:
                frac = float(os.environ.get("TCP_DISCONNECT_LATE_FRACTION", "0.95") or 0.95)
            except Exception:
                frac = 0.95
            try:
                measured_s = max(0.0, float(effective_run_seconds) - float(warmup_seconds))
                disconnect_is_late = measured_s >= (float(measure_seconds) * max(0.0, min(1.0, frac)))
            except Exception:
                disconnect_is_late = False

        if disconnect_is_late:
            result["note"] = f"tcp_source_disconnected_late:{result['tcp_source_disconnected_returncode']}"
        else:
            result["error"] = f"tcp_source_disconnected_early:{result['tcp_source_disconnected_returncode']}"

        if early_disconnect_logs:
            result.update(early_disconnect_logs)
    if empty_output_logs and not result.get("error"):
        result.update(empty_output_logs)
    if worker_state:
        result["nes_worker_state"] = worker_state
        # If the worker is not running, surface it as a hard failure and capture logs
        # before docker compose teardown removes the container.
        status_txt = str(worker_state.get("status") or "")
        if status_txt and status_txt != "running" and not result.get("error"):
            result["error"] = "nes_worker_not_running"
        if status_txt and status_txt != "running":
            try:
                wid = worker_container_id or docker_compose_service_id("nes-worker")
                if wid:
                    result.setdefault("nes_worker_logs_tail", docker_logs(wid))
            except Exception:
                pass

    if not cfg.output_csv.is_file():
        # Preserve any earlier error; only set output_csv_not_found when nothing else failed.
        if not result.get("error"):
            result["error"] = "output_csv_not_found"
        if missing_output_logs:
            result.update(missing_output_logs)
        return result

    # Prefer emit-timestamped output for event-time latency: L = EMIT_TS - END.
    # Only use emit_csv if it contains at least one valid data row; otherwise,
    # fall back to the raw sink output and surface emit-timestamp absence via
    # `emit_timestamps_missing` below (instead of incorrectly reporting a timeout).
    def _emit_csv_has_data_rows(path: Path) -> bool:
        if not path.is_file():
            return False
        try:
            with path.open("r", encoding="utf-8", newline="") as f:
                r = csv.reader(f)
                for row in r:
                    if not row:
                        continue
                    if cfg.start_col >= len(row) or cfg.end_col >= len(row):
                        continue
                    emit_col = len(row) - 1
                    if emit_col < 0 or emit_col >= len(row):
                        continue
                    try:
                        int(float(row[cfg.start_col]))
                        int(float(row[cfg.end_col]))
                        int(float(row[emit_col]))
                    except Exception:
                        continue
                    return True
        except Exception:
            return False
        return False

    source_stats_obj: Dict[str, Any] = {}
    if source_stats.is_file():
        try:
            source_stats_obj = json.loads(source_stats.read_text(encoding="utf-8"))
            result["source_stats"] = source_stats_obj
        except Exception:
            source_stats_obj = {}

    # Use the effective runtime as the upper bound for measurement duration.
    # This avoids under-reporting throughput/latency denominators when the TCP driver
    # disconnects early and the run ends before the configured measure window elapses.
    metrics_input: Path = cfg.output_csv
    try:
        actual_measure_seconds = max(0.0, float(effective_run_seconds) - float(warmup_seconds))
        metrics_run_seconds = (
            min(float(measure_seconds), actual_measure_seconds) if measure_seconds is not None else actual_measure_seconds
        )
        use_emit_csv = _emit_csv_has_data_rows(emit_csv)
        metrics_input = emit_csv if use_emit_csv else cfg.output_csv
        min_emit_ms = None
        if use_emit_csv and warmup_seconds and float(warmup_seconds) > 0.0:
            try:
                start_ms = int(source_stats_obj.get("stream_start_wall_ms") or 0)
                if start_ms > 0:
                    min_emit_ms = start_ms + int(float(warmup_seconds) * 1000.0)
            except Exception:
                min_emit_ms = None

        metrics = collect_metrics(
            input_csv=metrics_input,
            start_col=cfg.start_col,
            end_col=cfg.end_col,
            run_seconds=metrics_run_seconds if metrics_run_seconds > 0 else effective_run_seconds,
            query_name=f"{cfg.name}-stream",
            emit_col=-1 if metrics_input == emit_csv else None,
            min_emit_ms=min_emit_ms,
        )
        result["metrics"] = metrics
        # The benchmarking setup expects measured event-time latency (Karimov et al., ICDE'18):
        # L = EMIT_TS - window_end. If we lack the sink-side emission timestamps (emit_csv),
        # edbt_collect_metrics falls back to a synthetic latency model (window/2), which is
        # not comparable and can produce misleading constants (e.g., 5000ms for 10s windows).
        if (
            metrics_input != emit_csv
            and not result.get("error")
            and bool(metrics.get("has_data"))
            and int(metrics.get("num_rows") or 0) > 0
        ):
            result["error"] = "emit_timestamps_missing"
            try:
                if recorder_log.is_file():
                    tail = "\n".join(recorder_log.read_text(encoding="utf-8", errors="replace").splitlines()[-120:])
                    if tail.strip():
                        result["sink_recorder_logs_tail"] = tail
            except Exception:
                pass
    except Exception as exc:
        # Never crash the outer runner (edbt_runner/run_query.sh). Surface a stable error string instead.
        result.setdefault("metrics", {"has_data": False, "num_rows": 0})
        result["error"] = f"metrics_failed:{type(exc).__name__}:{exc}"
        return result
    geo_metrics = _read_json_file(geo_metrics_path)
    if geo_metrics:
        result["geo_function_metrics"] = geo_metrics
    geo_operator_metrics = _read_json_file(geo_operator_metrics_path)
    if geo_operator_metrics:
        result["geo_operator_metrics"] = geo_operator_metrics
    tgeo_metrics = _read_json_file(tgeo_metrics_path)
    if tgeo_metrics:
        result["tgeo_at_stbox_metrics"] = tgeo_metrics
    elif isinstance(geo_metrics, dict):
        tgeo_from_geo = geo_metrics.get("tgeo_at_stbox")
        if isinstance(tgeo_from_geo, dict) and tgeo_from_geo:
            result["tgeo_at_stbox_metrics"] = tgeo_from_geo

    # Processing-time latency proxy (Karimov et al., ICDE'18 Definition 2/4):
    #   processing_latency ≈ emit_wall_ms - ingest_wall_ms
    #
    # We do not have access to the SUT's internal ingest timestamp uniformly across all systems.
    # As a comparable proxy across Dockerized runners, we use the driver's last successful send
    # wall-clock time per event-time window end (recorded in tcp_source_csv_server stats).
    #
    # This captures queueing/backpressure between driver and SUT and isolates it from the
    # event-time definition (which uses window end time).
    def _pct(values: list[float], q: float) -> float | None:
        if not values:
            return None
        if q <= 0:
            return float(min(values))
        if q >= 1:
            return float(max(values))
        data = sorted(values)
        pos = (len(data) - 1) * q
        lo = int(pos)
        hi = min(len(data) - 1, lo + 1)
        frac = pos - lo
        return float(data[lo] * (1.0 - frac) + data[hi] * frac)

    proc_lat_ms: list[float] = []
    try:
        send_map_raw = (source_stats_obj.get("window_end_last_send_wall_ms") or {}) if isinstance(source_stats_obj, dict) else {}
        send_map: dict[int, int] = {}
        if isinstance(send_map_raw, dict):
            for k, v in send_map_raw.items():
                try:
                    send_map[int(k)] = int(v)
                except Exception:
                    continue

        if emit_csv.is_file() and send_map:
            with emit_csv.open("r", encoding="utf-8", newline="") as f:
                reader = csv.reader(f)
                for row in reader:
                    if not row:
                        continue
                    if cfg.end_col >= len(row) or len(row) < 1:
                        continue
                    try:
                        end_raw = int(float(row[cfg.end_col]))
                        emit_raw = int(float(row[-1]))
                    except Exception:
                        continue
                    emit_ms = emit_raw
                    if min_emit_ms is not None and emit_ms < int(min_emit_ms):
                        continue
                    # Normalize end (event-time window end) into epoch-ms for lookup.
                    end_ms = end_raw if end_raw >= 10_000_000_000 else (end_raw * 1000)
                    send_ms = send_map.get(int(end_ms))
                    if not send_ms or int(send_ms) <= 0:
                        continue
                    proc_lat_ms.append(float(max(0, emit_ms - int(send_ms))))
        if proc_lat_ms:
            result["proc_latency_ms"] = {
                "count": int(len(proc_lat_ms)),
                "p50_ms": _pct(proc_lat_ms, 0.50),
                "p95_ms": _pct(proc_lat_ms, 0.95),
            }
    except Exception:
        pass

    if docker_samples:
        cpu_vals = [s["cpu_percent"] for s in docker_samples if "cpu_percent" in s]
        mem_vals = [s["mem_mib"] for s in docker_samples if "mem_mib" in s]
        if cpu_vals and mem_vals:
            result["docker_stats"] = {
                "samples": len(docker_samples),
                "cpu_percent_avg": float(sum(cpu_vals) / len(cpu_vals)),
                "cpu_percent_peak": float(max(cpu_vals)),
                "mem_mib_avg": float(sum(mem_vals) / len(mem_vals)),
                "mem_mib_peak": float(max(mem_vals)),
            }
    elif docker_stats_interval_seconds and docker_stats_interval_seconds > 0:
        # Avoid running any docker commands here: by this point the stack may
        # already be torn down. Instead, surface the last errors captured by
        # the samplers for debugging.
        details: list[str] = []
        if docker_stats_last_error:
            details.append(f"docker_stats_last_error={docker_stats_last_error.strip()}")
        if cgroup_stats_last_error:
            details.append(f"cgroup_stats_last_error={cgroup_stats_last_error.strip()}")
        if not details:
            details.append("no_sampler_errors_captured")
        result["docker_stats_error"] = "no_samples; " + "; ".join(details)
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
    parser.add_argument(
        "--compose-override",
        action="append",
        default=[],
        help="Optional additional docker-compose YAML (repeatable). "
             "Example: --compose-override docker-compose.runtime.limits.yaml",
    )
    parser.add_argument(
        "--compose-compatibility",
        action="store_true",
        help="Pass --compatibility to docker compose (needed to enforce deploy.resources limits).",
    )
    parser.add_argument("--worker-threads", type=int, default=2,
                        help="NES_WORKER_THREADS for nes-worker container")
    parser.add_argument("--mode", type=str, choices=["csv", "stream"], default="csv",
                        help="Execution mode: 'csv' for file-based runs, "
                             "'stream' to drive tcp_source_csv_server at a fixed rate")
    parser.add_argument("--stream-input-csv", type=str, default="Input/selected_columns_df.csv",
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
    parser.add_argument(
        "--stream-ts-output-unit",
        type=str,
        choices=["s", "ms"],
        default="ms",
        help="Timestamp unit sent to the TCP source in streaming mode (default: ms).",
    )
    parser.add_argument(
        "--stream-rewrite-ts",
        type=str,
        choices=["none", "rate", "wall"],
        default="rate",
        help="Rewrite input event timestamps in streaming mode (default: rate). "
             "Use 'rate' to assign event-time at a constant speed derived from the configured rate.",
    )
    parser.add_argument(
        "--stream-emit-unit",
        type=str,
        choices=["ms", "s"],
        default="ms",
        help="Unit for the appended EMIT_TS column when recording sink emission time (default: ms).",
    )
    parser.add_argument("--stream-jitter-profile", type=str, default="",
                        help="Optional jitter profile, e.g. '10000,10;20000,10;40000,10' "
                             "for 10k/20k/40k e/s phases of 10s each. "
                             "If set, overrides --stream-rows-per-sec/--stream-run-seconds.")
    parser.add_argument(
        "--docker-stats-interval-seconds",
        type=float,
        default=0.0,
        help="If >0, sample `docker stats` for nes-worker every N seconds and report avg/peak CPU and memory.",
    )
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
    compose_overrides = [repo_root / p for p in (args.compose_override or [])]

    all_cfgs = build_default_query_matrix(repo_root)
    if args.queries:
        wanted = set(args.queries)
        cfgs = [c for c in all_cfgs if c.name in wanted]
    else:
        cfgs = all_cfgs

    consolidated: Dict[str, Any] = {
        "repo_root": str(repo_root),
        "compose_file": str(compose_file),
        "compose_overrides": [str(p) for p in compose_overrides],
        "compose_compatibility": bool(args.compose_compatibility),
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
                compose_overrides=compose_overrides,
                compose_compatibility=args.compose_compatibility,
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
                emit_unit=args.stream_emit_unit,
                rewrite_ts=args.stream_rewrite_ts,
                ts_output_unit=args.stream_ts_output_unit,
                compose_overrides=compose_overrides,
                compose_compatibility=args.compose_compatibility,
                docker_stats_interval_seconds=args.docker_stats_interval_seconds,
            )
        consolidated["queries"][cfg.name] = res

    out_path = repo_root / args.out
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(consolidated, f, indent=2, sort_keys=True)

    print(f"[INFO] Wrote consolidated metrics to {out_path}")


if __name__ == "__main__":
    main()
