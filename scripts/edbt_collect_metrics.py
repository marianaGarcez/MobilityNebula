#!/usr/bin/env python3
"""
Utility to compute basic throughput and latency summaries
from MobilityNebula CSV sink outputs.

It is generic over queries as long as the sink output has
numerical start/end columns (event-time window bounds).

Example (Q2 CSV):
  python3 scripts/edbt_collect_metrics.py \
    --input Output/output_query2.csv \
    --start-col 0 --end-col 1 \
    --run-seconds 30 \
    --query Q2 \
    --out Output/edbt/mobilitynebula_q2_metrics.json
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional


def _percentile(values: List[float], q: float) -> Optional[float]:
    """Compute the q-th percentile (0-100) using linear interpolation."""
    if not values:
        return None
    if q <= 0:
        return float(min(values))
    if q >= 100:
        return float(max(values))

    data = sorted(values)
    n = len(data)
    pos = (n - 1) * (q / 100.0)
    low = int(math.floor(pos))
    high = int(math.ceil(pos))
    if low == high:
        return float(data[low])
    frac = pos - low
    return float(data[low] * (1.0 - frac) + data[high] * frac)

def _mean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return float(sum(values) / len(values))


def collect_metrics(
    input_csv: Path | str,
    start_col: int,
    end_col: int,
    run_seconds: float | None = None,
    processing_ms: float = 0.0,
    sink_ms: float = 0.0,
    query_name: str | None = None,
    emit_col: int | None = None,
    min_emit_ms: int | None = None,
) -> Dict[str, Any]:
    """
    Compute simple metrics for a query output CSV.

    - input_csv: path to sink CSV file (no header assumed).
    - start_col, end_col: 0-based column indices of window start/end (epoch seconds).
    - run_seconds: optional wall-clock duration of the run. If omitted and
      emit_col is None, approximate as (max_end - min_start) in seconds.
      If emit_col is set and the column exists, approximate as
      (max_emit_ms - min_emit_ms) / 1000.
    - processing_ms, sink_ms:
        * If emit_col is None: constant per-window additions used when
          constructing synthetic total latency =
          window_wait + processing_ms + sink_ms.
        * If emit_col is set: processing_ms is interpreted as a constant
          processing contribution, and sink_ms is ignored; total latency
          is derived from the emit timestamp column instead.
    - emit_col: optional 0-based index of an emission timestamp column
      (epoch timestamp when the row was written/emitted; either seconds or ms).
      If set to -1, uses the last column of each row.
    - min_emit_ms: optional filter: when emit_col is set, ignore rows whose
      emission timestamp is < min_emit_ms (epoch milliseconds). Useful to
      exclude warmup from reported throughput/latency.
    - query_name: optional label (Q1..Q9).
    """
    latency_mode = "measured" if emit_col is not None else "synthetic"
    path = Path(input_csv)
    if not path.is_file():
        return {
            "query": query_name or "",
            "input_csv": str(path),
            "has_data": False,
            "latency_mode": latency_mode,
            "error": "input_csv_not_found",
        }

    window_durations_ms: List[float] = []
    window_wait_ms: List[float] = []
    min_start: Optional[int] = None
    max_end: Optional[int] = None
    min_emit_ms_seen: Optional[int] = None
    max_emit_ms_seen: Optional[int] = None
    num_rows = 0
    filtered_by_emit = 0
    time_is_millis: Optional[bool] = None

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            if start_col >= len(row) or end_col >= len(row):
                # Skip malformed rows rather than failing hard.
                continue
            try:
                start = int(row[start_col])
                end = int(row[end_col])
            except ValueError:
                # Non-numeric start/end, skip row.
                continue

            if max_end is None or end > max_end:
                max_end = end
            if min_start is None or start < min_start:
                min_start = start

            delta = max(0, end - start)
            if time_is_millis is None:
                # Infer time unit for start/end:
                # - Epoch milliseconds are ~1e12
                # - Epoch seconds are ~1e9
                #
                # Do not use a pure delta-based heuristic here: long windows in epoch
                # seconds (e.g., 10_000s) can be misclassified as milliseconds.
                abs_max = max(start, end)
                if abs_max >= 10_000_000_000:
                    time_is_millis = True
                elif abs_max >= 1_000_000_000:
                    time_is_millis = False
                else:
                    # For small/non-epoch values, fall back to a window-size heuristic.
                    time_is_millis = delta >= 1000
            scale_to_ms = 1.0 if time_is_millis else 1000.0
            duration_ms = float(delta) * scale_to_ms
            wait_ms = duration_ms / 2.0

            window_durations_ms.append(duration_ms)
            window_wait_ms.append(wait_ms)

            if emit_col is not None:
                effective_emit_col = (len(row) - 1) if emit_col == -1 else emit_col
                if effective_emit_col < 0 or effective_emit_col >= len(row):
                    # No emit timestamp column for this row; skip it entirely.
                    continue
                try:
                    emit_raw = int(row[effective_emit_col])
                except ValueError:
                    # Non-numeric emit timestamp; skip row.
                    continue
                # Keep emit timestamps in epoch-ms regardless of event-time unit, so we
                # preserve sub-second latency resolution when start/end are in seconds.
                emit_ms = emit_raw * 1000 if (0 < emit_raw < 10_000_000_000) else emit_raw

                if min_emit_ms is not None and emit_ms < int(min_emit_ms):
                    filtered_by_emit += 1
                    continue

                if max_emit_ms_seen is None or emit_ms > max_emit_ms_seen:
                    max_emit_ms_seen = emit_ms
                if min_emit_ms_seen is None or emit_ms < min_emit_ms_seen:
                    min_emit_ms_seen = emit_ms

            num_rows += 1

    if num_rows == 0:
        return {
            "query": query_name or "",
            "input_csv": str(path),
            "has_data": False,
            "latency_mode": latency_mode,
            "num_rows": 0,
        }

    # Derive run_seconds if not supplied.
    is_millis = bool(time_is_millis)
    divisor = 1000.0

    if run_seconds is None:
        if (
            emit_col is not None
            and min_emit_ms_seen is not None
            and max_emit_ms_seen is not None
            and max_emit_ms_seen > min_emit_ms_seen
        ):
            # Prefer wall-clock span if we have emit timestamps.
            run_seconds = float(max_emit_ms_seen - min_emit_ms_seen) / divisor
        elif min_start is not None and max_end is not None and max_end > min_start:
            # Fallback: event-time span.
            run_seconds = float((max_end - min_start) * (1 if is_millis else 1000)) / divisor
        else:
            run_seconds = 0.0

    throughput = float(num_rows) / run_seconds if run_seconds and run_seconds > 0 else 0.0

    # Latency model:
    #   If emit_col is None (synthetic):
    #       window_wait_ms  = (end - start) / 2
    #       processing_ms   = constant per-window (argument)
    #       sink_ms         = constant per-window (argument)
    #       total_latency   = sum of the above
    #   If emit_col is set (measured):
    #       total_latency   = (emit_ts - end) * 1000
    #       processing_ms   = constant per-window (argument)
    #       sink_ms         = residual = max(0, total_latency - window_wait_ms - processing_ms)
    total_latency_ms: List[float] = []
    processing_vals: List[float] = []
    sink_vals: List[float] = []
    negative_latency_rows = 0
    min_total_latency_raw: Optional[float] = None

    if emit_col is None:
        total_latency_ms = [w + processing_ms + sink_ms for w in window_wait_ms]
        processing_vals = [processing_ms] * num_rows
        sink_vals = [sink_ms] * num_rows
    else:
        window_wait_ms = []
        # We need to re-parse to compute per-row total latency using emit timestamps.
        # This keeps memory usage bounded and avoids storing per-row timestamps above.
        filtered_by_emit_latency = 0
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue
                effective_emit_col = (len(row) - 1) if emit_col == -1 else emit_col
                if (
                    start_col >= len(row)
                    or end_col >= len(row)
                    or effective_emit_col < 0
                    or effective_emit_col >= len(row)
                ):
                    continue
                try:
                    start = int(row[start_col])
                    end = int(row[end_col])
                    emit_raw = int(row[effective_emit_col])
                except ValueError:
                    continue

                delta = max(0, end - start)
                scale_to_ms = 1.0 if is_millis else 1000.0
                duration_ms = float(delta) * scale_to_ms
                wait_ms = duration_ms / 2.0
                # Normalize both end and emit into epoch-ms for latency.
                emit_ms = emit_raw * 1000 if (0 < emit_raw < 10_000_000_000) else emit_raw
                end_ms = end if is_millis else (end * 1000)

                if min_emit_ms is not None and emit_ms < int(min_emit_ms):
                    filtered_by_emit_latency += 1
                    continue

                raw_latency_ms = float(emit_ms - end_ms)
                if min_total_latency_raw is None or raw_latency_ms < min_total_latency_raw:
                    min_total_latency_raw = raw_latency_ms
                if raw_latency_ms < 0:
                    negative_latency_rows += 1
                latency_ms = max(0.0, raw_latency_ms)

                total_latency_ms.append(latency_ms)
                window_wait_ms.append(wait_ms)

                proc = max(0.0, processing_ms)
                sink = max(0.0, latency_ms - wait_ms - proc)

                processing_vals.append(proc)
                sink_vals.append(sink)

        # Keep num_rows consistent with the number of valid rows used in latency stats.
        num_rows = len(total_latency_ms)
        # Avoid double-counting: the second pass re-applies the same filter.
        filtered_by_emit = max(filtered_by_emit, filtered_by_emit_latency)

    metrics: Dict[str, Any] = {
        "query": query_name or "",
        "input_csv": str(path),
        "has_data": True,
        "latency_mode": latency_mode,
        "num_rows": num_rows,
        "filtered_by_emit": int(filtered_by_emit),
        "run_seconds": run_seconds,
        "throughput_rows_per_sec": throughput,
        "window_ms": {
            "avg": _mean(window_durations_ms),
            "min": float(min(window_durations_ms)),
            "max": float(max(window_durations_ms)),
            "p50": _percentile(window_durations_ms, 50.0),
            "p95": _percentile(window_durations_ms, 95.0),
        },
        "latency_ms": {
            "avg_total": _mean(total_latency_ms),
            "p50_total": _percentile(total_latency_ms, 50.0),
            "p95_total": _percentile(total_latency_ms, 95.0),
            "min_total": float(min(total_latency_ms)) if total_latency_ms else None,
            "max_total": float(max(total_latency_ms)) if total_latency_ms else None,
            # Debug aid: negative event-time latency indicates the output 'end' timestamp
            # is ahead of wall-clock emission time (usually due to an incompatible timestamp
            # rewrite mode). We clamp negatives to 0ms for reporting but surface counts here.
            "negative_total_rows": int(negative_latency_rows),
            "min_total_raw": float(min_total_latency_raw) if min_total_latency_raw is not None else None,
            "avg_window_wait": _mean(window_wait_ms),
            "p50_window_wait": _percentile(window_wait_ms, 50.0),
            "p95_window_wait": _percentile(window_wait_ms, 95.0),
            "avg_processing": _mean(processing_vals),
            "p50_processing": _percentile(processing_vals, 50.0),
            "p95_processing": _percentile(processing_vals, 95.0),
            "avg_sink": _mean(sink_vals),
            "p50_sink": _percentile(sink_vals, 50.0),
            "p95_sink": _percentile(sink_vals, 95.0),
        },
        "components_ms": {
            "processing_ms_constant": processing_ms,
            "sink_ms_constant": sink_ms,
        },
    }
    return metrics


def main() -> None:
    p = argparse.ArgumentParser(
        description="Compute simple throughput and latency summaries "
                    "from a MobilityNebula CSV sink output."
    )
    p.add_argument("--input", required=True, help="Path to sink CSV file")
    p.add_argument("--start-col", type=int, required=True, help="0-based index of window start column")
    p.add_argument("--end-col", type=int, required=True, help="0-based index of window end column")
    p.add_argument("--run-seconds", type=float, default=None,
                   help="Wall-clock run duration; if omitted, approximated from event-time span")
    p.add_argument("--processing-ms", type=float, default=0.0,
                   help="Constant processing contribution per window (ms)")
    p.add_argument("--sink-ms", type=float, default=0.0,
                   help="Constant sink contribution per window (ms)")
    p.add_argument("--emit-col", type=int, default=None,
                   help="Optional 0-based index of an emission timestamp column "
                        "(epoch seconds when the row was written/emitted)")
    p.add_argument("--min-emit-ms", type=int, default=None,
                   help="If set (and --emit-col is set), ignore rows with emit_ts_ms < this epoch-ms threshold")
    p.add_argument("--query", type=str, default=None, help="Query label (e.g., Q2, Q5)")
    p.add_argument("--out", type=str, default=None, help="Optional JSON output path")

    args = p.parse_args()

    metrics = collect_metrics(
        input_csv=Path(args.input),
        start_col=args.start_col,
        end_col=args.end_col,
        run_seconds=args.run_seconds,
        processing_ms=args.processing_ms,
        sink_ms=args.sink_ms,
        query_name=args.query,
        emit_col=args.emit_col,
        min_emit_ms=args.min_emit_ms,
    )

    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2, sort_keys=True)

    # Also print a compact summary to stdout for quick inspection.
    print(json.dumps(metrics, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
