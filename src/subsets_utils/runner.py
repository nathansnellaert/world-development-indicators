#!/usr/bin/env python3
"""
Supervisor/Runner for cloud execution.

This is the entry point for GitHub Actions. It acts as the "OS" for the connector:
handles subprocess management, memory profiling, signal handling, and log evacuation.

Usage:
    python -m subsets_utils.runner
    python -m subsets_utils.runner --ingest-only
"""

import argparse
import csv
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

from .r2 import upload_bytes, upload_file, is_cloud_mode
from . import debug


class MemoryProfiler:
    """External memory profiler that monitors a subprocess from the parent."""

    def __init__(self, pid: int, log_dir: Path, interval: float = 0.5):
        self.pid = pid
        self.log_file = log_dir / "memory.csv"
        self.interval = interval
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)

    def _sample_loop(self):
        try:
            import psutil
        except ImportError:
            print("Warning: psutil not available, memory profiling disabled")
            return

        try:
            process = psutil.Process(self.pid)
        except psutil.NoSuchProcess:
            return

        # Write CSV header
        with open(self.log_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "rss_mb", "vms_mb", "pct"])

        while not self._stop.is_set():
            try:
                # Get memory for process and all children
                rss = process.memory_info().rss
                vms = process.memory_info().vms
                pct = process.memory_percent()

                for child in process.children(recursive=True):
                    try:
                        rss += child.memory_info().rss
                        vms += child.memory_info().vms
                        pct += child.memory_percent()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass

                with open(self.log_file, 'a', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        datetime.now().isoformat(),
                        round(rss / 1024 / 1024, 1),
                        round(vms / 1024 / 1024, 1),
                        round(pct, 1)
                    ])

            except (psutil.NoSuchProcess, psutil.AccessDenied):
                break

            self._stop.wait(self.interval)


def upload_logs(log_dir: Path, run_id: str, connector_name: str):
    """Upload all log files to R2."""
    if not log_dir.exists():
        print("No logs to upload")
        return

    print(f"Uploading logs to R2...")
    for log_file in log_dir.rglob('*'):
        if not log_file.is_file():
            continue
        try:
            key = f"{connector_name}/logs/{run_id}/{log_file.relative_to(log_dir)}"
            upload_file(str(log_file), key)
            print(f"  -> {key}")
        except Exception as e:
            print(f"  Failed to upload {log_file.name}: {e}")


def write_error_log(log_dir: Path, exit_code: int, output_file: Path, tail_lines: int = 100):
    """Write the last N lines of output as error.txt."""
    error_file = log_dir / "error.txt"

    if not output_file.exists():
        with open(error_file, 'w') as f:
            f.write(f"Exit code: {exit_code}\nNo output captured.\n")
        return

    # Read last N lines
    with open(output_file, 'r') as f:
        lines = f.readlines()

    tail = lines[-tail_lines:] if len(lines) > tail_lines else lines

    with open(error_file, 'w') as f:
        f.write(f"Exit code: {exit_code}\n")
        f.write(f"Last {len(tail)} lines of output:\n")
        f.write("-" * 60 + "\n")
        f.writelines(tail)


def main():
    parser = argparse.ArgumentParser(description="Run connector under supervision")
    parser.add_argument("--ingest-only", action="store_true", help="Only run ingestion")
    args = parser.parse_args()

    # Detect connector name from cwd (e.g., /path/to/integrations/accelerators -> accelerators)
    connector_name = Path.cwd().name
    os.environ['CONNECTOR_NAME'] = connector_name

    # Setup
    run_id = os.environ.get('RUN_ID', datetime.now(ZoneInfo('UTC')).strftime('%Y%m%d-%H%M%S'))

    # Log directory: local uses connector's logs/, cloud uses /tmp/logs/
    if is_cloud_mode():
        log_dir = Path("/tmp/logs") / run_id
    else:
        log_dir = Path("logs") / run_id
    log_dir.mkdir(parents=True, exist_ok=True)

    # Set LOG_DIR so debug.py writes here
    os.environ['LOG_DIR'] = str(log_dir)

    # Build command
    cmd = [sys.executable, "-m", "src.main"]
    if args.ingest_only:
        cmd.append("--ingest-only")

    print(f"Starting connector (RUN_ID: {run_id})")
    print(f"Log directory: {log_dir}")
    print(f"Command: {' '.join(cmd)}")
    print("-" * 60)

    # Log run start
    debug.log_run_start()

    # Set up environment with src in PYTHONPATH so 'from subsets_utils' works
    env = os.environ.copy()
    src_path = str(Path.cwd() / "src")
    env["PYTHONPATH"] = src_path + (":" + env["PYTHONPATH"] if "PYTHONPATH" in env else "")

    # Output file for capturing logs (used for error.txt on failure)
    output_file = log_dir / "output.log"

    # Start subprocess with output captured to file AND streamed to console
    with open(output_file, 'w') as log_f:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            text=True,
            bufsize=1  # Line buffered
        )

        # Start memory profiler
        profiler = MemoryProfiler(process.pid, log_dir)
        profiler.start()

        # Signal handler for SIGTERM (GitHub timeout)
        def handle_sigterm(signum, frame):
            print(f"\nReceived SIGTERM, terminating child...")
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()

        signal.signal(signal.SIGTERM, handle_sigterm)

        # Stream output to both console and file
        try:
            for line in process.stdout:
                sys.stdout.write(line)
                sys.stdout.flush()
                log_f.write(line)
                log_f.flush()
        except KeyboardInterrupt:
            print("\nInterrupted, terminating child...")
            process.terminate()

        exit_code = process.wait()

    # Stop profiler
    profiler.stop()

    # Handle exit
    print("-" * 60)

    if exit_code == 0:
        print(f"Connector completed successfully")
        debug.log_run_end(status="completed")
    elif exit_code == 137:
        print(f"Connector killed by OOM (exit code 137)")
        write_error_log(log_dir, exit_code, output_file)
        debug.log_run_end(status="oom", error="Exit code 137 - Out of memory")
    elif exit_code == 143:
        print(f"Connector terminated by SIGTERM (exit code 143)")
        write_error_log(log_dir, exit_code, output_file)
        debug.log_run_end(status="timeout", error="Exit code 143 - SIGTERM")
    else:
        print(f"Connector failed with exit code {exit_code}")
        write_error_log(log_dir, exit_code, output_file)
        debug.log_run_end(status="failed", error=f"Exit code {exit_code}")

    # Always upload logs
    if is_cloud_mode():
        upload_logs(log_dir, run_id, connector_name)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
