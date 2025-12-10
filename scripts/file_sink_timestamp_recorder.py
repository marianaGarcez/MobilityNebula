#!/usr/bin/env python3
"""
Tail a MobilityNebula file sink CSV and append a wall-clock emission
timestamp column to each data row.

Usage (example for Query2 TCP run):

  # Terminal 1: start recorder before or just after starting the query
  python3 scripts/file_sink_timestamp_recorder.py \
    --input Output/output_query2.csv \
    --out Output/output_query2_emit.csv

  # Terminal 2: run the query in Docker (e.g. via run_edbt_matrix.py)

When the query finishes, Output/output_query2_emit.csv will contain
the original rows plus one extra EMIT_TS column (epoch seconds).
You can then call edbt_collect_metrics.py with --emit-col to compute
true end-to-end latency L = EMIT_TS - end.
"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path


def tail_with_timestamps(input_path: Path, out_path: Path, poll_interval: float = 0.05) -> None:
    """
    Follow input_path as it is written by the File sink and write a copy
    to out_path with an extra EMIT_TS column containing time.time() in
    epoch seconds for each line.

    This script is intentionally simple: it assumes a single writer and
    appends timestamps as lines appear. Stop it with Ctrl+C once your
    query has finished.
    """
    print(f"[INFO] Watching file sink: {input_path}")
    print(f"[INFO] Writing timestamped copy to: {out_path}")

    # Wait for the input file to appear.
    while not input_path.exists():
        time.sleep(poll_interval)

    # Open input for reading and output for writing (truncate/overwrite).
    with input_path.open("r", encoding="utf-8", newline="") as src, \
            out_path.open("w", encoding="utf-8", newline="") as dst:
        # Start reading from the beginning; we may see a header first.
        position = 0
        src.seek(0, os.SEEK_SET)

        while True:
            src.seek(position, os.SEEK_SET)
            line = src.readline()
            if not line:
                # No new data yet; wait and retry.
                time.sleep(poll_interval)
                continue

            position = src.tell()
            stripped = line.rstrip("\n\r")

            # Derive EMIT_TS and append as extra CSV field.
            emit_ts = int(time.time())
            if stripped:
                dst.write(f"{stripped},{emit_ts}\n")
                dst.flush()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Tail a MobilityNebula File sink CSV and append an EMIT_TS column "
                    "with wall-clock emission timestamps."
    )
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Path to the File sink CSV to watch (e.g., Output/output_query2.csv)",
    )
    parser.add_argument(
        "--out",
        type=str,
        required=True,
        help="Path to the output CSV with EMIT_TS appended (e.g., Output/output_query2_emit.csv)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=0.05,
        help="Polling interval in seconds while waiting for new lines (default: 0.05)",
    )

    args = parser.parse_args()
    input_path = Path(args.input)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        tail_with_timestamps(input_path, out_path, poll_interval=args.poll_interval)
    except KeyboardInterrupt:
        print("\n[INFO] Stopped timestamp recorder.")


if __name__ == "__main__":
    main()

