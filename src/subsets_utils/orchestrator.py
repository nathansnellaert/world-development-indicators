"""DAG orchestration with run-state persistence and resume.

The DAG class:
- Builds a topological order from `nodes` dict (`{fn: [deps]}`)
- Optionally inherits state from a prior run.json (resume across invocations)
- Runs each node in a fresh forked subprocess (memory isolation per node)
- Writes run.json after each node
- Marks status as "needs_continuation" if any node returns True (pagination)
- Knows nothing about exit codes or time budgets — that's runner.py's job.

Subprocess-per-node:
- Each node is executed in a forked child process via multiprocessing.
- Child runs one fn(), pipes back a result dict, exits. OS reclaims RSS.
- One node OOMing only kills that node; the rest of the DAG continues.
- Tracking state (asset_writers, io_records) is serialized by the child and
  merged into the supervisor's tracking module after each node completes.

Continuation pattern:
- A node returns True to signal "more work to do, please retrigger me later"
- The orchestrator records this in run.json status field
- Runner.py reads run.json after subprocess exit and translates the status
  to an exit code (0=done, 2=continuation, 1=failed)
- A SIGTERM that interrupts the supervisor does NOT trigger continuation —
  the run is marked failed, and a human must investigate. Auto-retrigger
  on host kill would loop forever on a real OOM root cause.

Resume pattern:
- LOG_DIR/run.json exists from a prior invocation → load it
- Topology hash matches → inherit "done" status for matching nodes
- Topology hash differs → log warning, ignore prior state, run fresh
"""

import hashlib
import importlib.util
import json
import multiprocessing
import os
import pickle
import signal
import sys
import tempfile
import time
import traceback
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from . import tracking
from .tracking import (
    IORecord,
    clear_tracking,
    get_asset_version,
    get_assets_by_writer,
    get_reads_by_task,
    set_current_task,
)


# Fork context for subprocess-per-node execution. Fork is fast (~10ms via CoW)
# and lets the child inherit the supervisor's loaded modules and DAG metadata
# without re-importing anything. Linux + macOS supported (our stack —
# pyarrow/fsspec/requests/deltalake — does not touch fork-unsafe Apple APIs).
_MP_CTX = multiprocessing.get_context("fork")

# Cap on the pickled size of a child→supervisor result dict. Defends against a
# node accidentally stuffing a large pa.Table into a tracking record. 10 MB is
# generous: tracking records are tiny strings and stack snippets.
_MAX_RESULT_PICKLE_BYTES = 10 * 1024 * 1024


def _get_task_id(fn: Callable) -> str:
    """Get unique task ID from function (module.name)."""
    module = fn.__module__
    if module.startswith("src."):
        module = module[4:]
    return f"{module}.{fn.__name__}"


def _topology_hash(nodes: dict) -> str:
    """Hash of DAG topology — used to detect changes between invocations."""
    items = sorted(
        (
            _get_task_id(fn),
            sorted(_get_task_id(d) for d in deps),
        )
        for fn, deps in nodes.items()
    )
    return hashlib.md5(json.dumps(items).encode()).hexdigest()[:16]


def _atomic_write_json(path: Path, data: dict) -> None:
    """Write JSON atomically (write to tmp, rename) so partial writes can't corrupt."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2)
        os.rename(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _load_run_state(log_dir: Path) -> dict | None:
    """Load run.json from a log directory, or None if missing/invalid."""
    p = log_dir / "run.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def _child_entrypoint(fn: Callable, task_id: str, pipe_w) -> None:
    """Runs in a forked child process. Executes one DAG node and pipes back
    a result dict. The child inherits the supervisor's modules and tracking
    dicts via fork; we clear tracking on entry so the snapshot we send back
    contains only this node's I/O.

    The result dict shape:
        {
            "task_id": str,
            "status": "done" | "failed",
            "started_at": iso8601 str,
            "finished_at": iso8601 str,
            "duration_s": float,
            "needs_continuation": bool,        # only when status == "done"
            "error": str (only on failed),
            "traceback": str (only on failed),
            "tracking": {
                "asset_writers": {asset_path: task_id},
                "asset_versions": {asset_path: {"version": int, "hash": str}},
                "io_records": [{"asset_path", "task_id", "operation", "stack"}],
            }
        }
    """
    # Reset signal handlers in the child — supervisor's SIGTERM handler is
    # CoW-inherited but does not apply to children. Default disposition for
    # SIGTERM is "terminate" which is what we want when supervisor escalates.
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    clear_tracking()
    set_current_task(task_id)

    started_at = datetime.now(timezone.utc).isoformat()
    result: dict = {
        "task_id": task_id,
        "started_at": started_at,
        "status": "failed",
        "needs_continuation": False,
    }

    try:
        ret = fn()
        result["status"] = "done"
        if ret is True:
            result["needs_continuation"] = True
    except BaseException as e:  # noqa: BLE001 — surface every failure mode
        result["status"] = "failed"
        result["error"] = str(e) or e.__class__.__name__
        result["traceback"] = traceback.format_exc()

    finished_at = datetime.now(timezone.utc).isoformat()
    result["finished_at"] = finished_at
    try:
        result["duration_s"] = (
            datetime.fromisoformat(finished_at) - datetime.fromisoformat(started_at)
        ).total_seconds()
    except Exception:
        result["duration_s"] = 0.0

    result["tracking"] = {
        "asset_writers": dict(tracking._asset_writers),
        "asset_versions": dict(tracking._asset_versions),
        "io_records": [asdict(r) for r in tracking._io_records],
    }

    # Flush stdio before sending result. Fork-inherited pipes can drop the
    # last buffered line if the child exits without flushing.
    sys.stdout.flush()
    sys.stderr.flush()

    try:
        payload = pickle.dumps(result)
        if len(payload) > _MAX_RESULT_PICKLE_BYTES:
            raise ValueError(
                f"result too large ({len(payload)} bytes > {_MAX_RESULT_PICKLE_BYTES}); "
                "a node likely stashed a large object in tracking"
            )
        pipe_w.send_bytes(payload)
    except Exception as e:  # serialization or pipe write failure
        try:
            fallback = pickle.dumps({
                "task_id": task_id,
                "started_at": started_at,
                "finished_at": finished_at,
                "duration_s": result.get("duration_s", 0.0),
                "status": "failed",
                "error": f"failed to serialize result: {e}",
                "traceback": traceback.format_exc(),
                "needs_continuation": False,
                "tracking": {"asset_writers": {}, "asset_versions": {}, "io_records": []},
            })
            pipe_w.send_bytes(fallback)
        except Exception:
            pass
    finally:
        try:
            pipe_w.close()
        except Exception:
            pass


class DAG:
    def __init__(self, nodes: dict[Callable, list[Callable]]):
        self.nodes = nodes
        self.state: dict[str, dict] = {}
        self._fn_to_id: dict[Callable, str] = {}
        self._id_to_fn: dict[str, Callable] = {}
        self._needs_continuation = False
        self._shutdown_requested = False
        self.topology_hash = _topology_hash(nodes)

        for fn in nodes:
            task_id = _get_task_id(fn)
            self._fn_to_id[fn] = task_id
            self._id_to_fn[task_id] = fn
            self.state[task_id] = {
                "id": task_id,
                "deps": [_get_task_id(d) for d in nodes[fn]],
                "status": "pending",
                "started_at": None,
                "finished_at": None,
                "duration_s": None,
                "error": None,
                "raw_reads": [],
                "raw_writes": [],
                "subsets_reads": [],
                "materializations": [],
            }

        # Try to inherit state from a prior run if LOG_DIR has a run.json
        log_dir = os.environ.get("LOG_DIR")
        if log_dir:
            prior = _load_run_state(Path(log_dir))
            if prior is not None:
                self._inherit_from(prior)

    # =========================================================================
    # Resume
    # =========================================================================

    def _inherit_from(self, prior: dict) -> None:
        """Inherit done node states from a prior run.json (if topology matches)."""
        prior_hash = prior.get("topology_hash")
        if prior_hash and prior_hash != self.topology_hash:
            print(
                f"[DAG] Topology hash mismatch with prior run "
                f"({prior_hash} vs {self.topology_hash}); starting fresh"
            )
            return

        prior_nodes = {n["id"]: n for n in prior.get("dag", {}).get("nodes", [])}
        inherited = 0
        for task_id, prior_state in prior_nodes.items():
            if task_id in self.state and prior_state.get("status") == "done":
                # Preserve "done" with all its tracking + timing data
                self.state[task_id] = {
                    **self.state[task_id],
                    **prior_state,
                    "resumed": True,
                }
                inherited += 1
        if inherited:
            print(f"[DAG] Resumed from prior invocation: {inherited} nodes already done")

    # =========================================================================
    # Topology
    # =========================================================================

    def _topological_order(self) -> list[Callable]:
        """Return functions in dependency order (Kahn's algorithm)."""
        in_degree = {fn: len(deps) for fn, deps in self.nodes.items()}
        ready = [fn for fn, deg in in_degree.items() if deg == 0]
        order: list[Callable] = []

        while ready:
            fn = ready.pop(0)
            order.append(fn)
            for other_fn, deps in self.nodes.items():
                if fn in deps:
                    in_degree[other_fn] -= 1
                    if in_degree[other_fn] == 0:
                        # Insert at FRONT to run dependent immediately (DFS-style),
                        # so download→transform pairs run together.
                        ready.insert(0, other_fn)

        if len(order) != len(self.nodes):
            raise ValueError("Cycle detected in DAG")
        return order

    # =========================================================================
    # Execution
    # =========================================================================

    def _spawn_task(self, fn: Callable):
        """Fork a child process to run one node. Returns (proc, pipe_r).

        The supervisor closes its copy of the pipe write-end after fork so the
        read-end sees a clean EOF if the child dies without sending. The child
        inherits the read-end too but never uses it; that's harmless.
        """
        task_id = self._fn_to_id[fn]
        pipe_r, pipe_w = _MP_CTX.Pipe(duplex=False)
        proc = _MP_CTX.Process(
            target=_child_entrypoint,
            args=(fn, task_id, pipe_w),
            name=f"node:{task_id}",
        )
        proc.start()
        # After fork, the child holds its own ref to pipe_w. The supervisor
        # must drop its copy so the pipe closes cleanly on child exit.
        pipe_w.close()
        return proc, pipe_r

    def _collect_result(self, proc: multiprocessing.Process, pipe_r) -> dict:
        """Join a child proc and read the result dict it sent. If the child
        died before sending (OOM SIGKILL, segfault, etc.), synthesize a failure
        result based on its exit code."""
        proc.join()

        result: dict | None = None
        if pipe_r.poll():
            try:
                result = pickle.loads(pipe_r.recv_bytes())
            except Exception as e:
                result = None
        try:
            pipe_r.close()
        except Exception:
            pass

        if result is not None:
            return result

        # Child died without sending a result.
        exitcode = proc.exitcode
        if exitcode is None:
            error = "child still alive after join (should not happen)"
        elif exitcode < 0:
            try:
                signame = signal.Signals(-exitcode).name
            except (ValueError, AttributeError):
                signame = f"signal {-exitcode}"
            error = f"killed by {signame} (exitcode={exitcode}); likely OOM or external kill"
        else:
            error = f"child exited with code {exitcode} before sending result"

        now = datetime.now(timezone.utc).isoformat()
        return {
            "task_id": proc.name.split(":", 1)[-1] if ":" in proc.name else proc.name,
            "status": "failed",
            "error": error,
            "traceback": "",
            "started_at": now,
            "finished_at": now,
            "duration_s": 0.0,
            "needs_continuation": False,
            "tracking": {"asset_writers": {}, "asset_versions": {}, "io_records": []},
        }

    def _apply_result(self, task_id: str, result: dict) -> None:
        """Merge a child result dict into self.state and the tracking module."""
        task_state = self.state[task_id]
        task_state["status"] = result["status"]
        task_state["started_at"] = result.get("started_at")
        task_state["finished_at"] = result.get("finished_at")
        task_state["duration_s"] = result.get("duration_s")
        if result["status"] == "failed":
            task_state["error"] = result.get("error", "unknown")
            task_state["traceback"] = result.get("traceback", "")
        elif result.get("needs_continuation"):
            task_state["needs_continuation"] = True
            self._needs_continuation = True

        # Merge child's tracking snapshot into the supervisor's tracking module
        # so to_json() and _print_node_detail() see this node's I/O.
        snapshot = result.get("tracking") or {}
        with tracking._lock:
            tracking._asset_writers.update(snapshot.get("asset_writers", {}))
            tracking._asset_versions.update(snapshot.get("asset_versions", {}))
            for r in snapshot.get("io_records", []):
                tracking._io_records.append(IORecord(**r))

    def run(self, targets: list[str] | None = None):
        """Execute all nodes in dependency order, each in its own forked
        subprocess. Writes run.json after every node.

        Args:
            targets: Optional list of node names to run (assumes deps already ran).

        Env vars:
            DAG_TARGET: Comma-separated node names to run (overrides `targets`).
            DAG_ON_FAILURE: "crash" (default) or "continue".
            DAG_PARALLELISM: Max concurrent nodes (default 1 = sequential).
            DAG_DRAIN_TIMEOUT_S: Max seconds to wait for children on SIGTERM (default 8).

        Behavior:
            - Each node runs in a fresh forked child; OS reclaims RSS on exit.
            - A node OOM (SIGKILL) only fails that node; the rest of the DAG
              continues unless DAG_ON_FAILURE=crash.
            - On a node returning True: marks needs_continuation, continues running.
            - On node failure with crash mode: drains in-flight tasks, then raises.
            - On node failure with continue mode: raises after all nodes complete.
            - On SIGTERM: drains in-flight, marks remaining as failed, schreef
              run.json with status="failed". No auto-retrigger from host kill.
            - The DAG class never calls sys.exit() — exit codes are runner.py's job.
        """
        clear_tracking()

        on_failure = os.environ.get("DAG_ON_FAILURE", "crash")
        try:
            parallelism = max(1, int(os.environ.get("DAG_PARALLELISM", "1")))
        except ValueError:
            parallelism = 1
        env_targets = os.environ.get("DAG_TARGET")
        if env_targets:
            targets = [t.strip() for t in env_targets.split(",")]

        order = self._topological_order()

        if targets:
            target_set = set(targets)
            order = [
                fn for fn in order
                if self._fn_to_id[fn].split(".")[-2] in target_set
            ]
            if not order:
                # Fall back to matching full task_id or function name
                order = [
                    fn for fn in self._topological_order()
                    if self._fn_to_id[fn] in target_set or fn.__name__ in target_set
                ]
            if not order:
                print(f"[DAG] No nodes matched targets: {targets}")
                print(f"[DAG] Available: {[self._fn_to_id[fn] for fn in self.nodes]}")
                self.save_state()
                return self
            # Mark all non-targeted nodes as "skipped" so the run can finalize as
            # done even though only a subset executed.
            targeted_ids = {self._fn_to_id[fn] for fn in order}
            for task_id, st in self.state.items():
                if task_id not in targeted_ids and st["status"] == "pending":
                    st["status"] = "skipped"
                    st["error"] = "Not in DAG_TARGET"

        # Log resumed nodes upfront so output ordering matches prior behavior
        for fn in order:
            task_id = self._fn_to_id[fn]
            if self.state[task_id]["status"] == "done":
                print(f"[DAG] {task_id} resumed (done in prior invocation)")

        first_failure = None
        stop_submitting = False

        def find_ready() -> list[Callable]:
            """Return pending nodes whose deps are done, in topological order.
            Skips nodes whose deps already failed (marks them skipped)."""
            ready: list[Callable] = []
            for fn in order:
                task_id = self._fn_to_id[fn]
                if self.state[task_id]["status"] != "pending":
                    continue
                dep_states = [
                    self.state[self._fn_to_id[dep]]["status"]
                    for dep in self.nodes[fn]
                ]
                if any(s in ("failed", "skipped") for s in dep_states):
                    self.state[task_id]["status"] = "skipped"
                    self.state[task_id]["error"] = "Upstream dependency did not complete"
                    self.save_state()
                    continue
                if all(s == "done" for s in dep_states):
                    ready.append(fn)
            return ready

        # Each node runs in its own forked subprocess so memory is reclaimed
        # between nodes. in_flight maps a live Process to its (task_id, pipe_r).
        in_flight: dict[multiprocessing.Process, tuple[str, object]] = {}

        # SIGTERM policy depends on DAG_ON_FAILURE:
        #
        # - "continue": ignore SIGTERM entirely. GitHub Actions sometimes sends
        #   SIGTERM to the whole step when a child OOMs and the host briefly
        #   thrashes — but the OOM killer already reaped the offending child,
        #   so we can keep going. If GH really wants us dead it sends SIGKILL
        #   ~10s after SIGTERM, which we cannot catch, and the step dies hard.
        #   That is acceptable: save_state runs after every node so at most a
        #   few seconds of progress is lost.
        #
        # - "crash" (default): drain in-flight, mark pending, exit. Used by
        #   callers who want a single failure to halt the run cleanly.
        prior_handler = signal.getsignal(signal.SIGTERM)
        ignore_sigterm = (on_failure == "continue")

        def _on_sigterm(signum, frame):
            nonlocal stop_submitting
            if ignore_sigterm:
                print("[DAG] Received SIGTERM (ignored — DAG_ON_FAILURE=continue)")
                return
            print("[DAG] Received SIGTERM, draining in-flight nodes...")
            stop_submitting = True
            self._shutdown_requested = True

        try:
            signal.signal(signal.SIGTERM, _on_sigterm)
        except ValueError:
            # signal.signal can only be called from the main thread; if we're
            # in a worker thread we just skip — the supervisor is normally main.
            pass

        def submit_more():
            if stop_submitting:
                return
            for fn in find_ready():
                if len(in_flight) >= parallelism:
                    return
                task_id = self._fn_to_id[fn]
                # Reserve the slot before fork so the next find_ready() doesn't
                # see this node as pending and re-spawn it.
                self.state[task_id]["status"] = "running"
                self.state[task_id]["started_at"] = datetime.now(timezone.utc).isoformat()
                print(f"[DAG] Running {task_id}...")
                proc, pipe_r = self._spawn_task(fn)
                in_flight[proc] = (task_id, pipe_r)

        def collect_one(proc: multiprocessing.Process) -> dict:
            """Pop a finished proc, collect its result, apply, save_state."""
            task_id, pipe_r = in_flight.pop(proc)
            result = self._collect_result(proc, pipe_r)
            self._apply_result(task_id, result)
            self.save_state()
            return result

        try:
            submit_more()

            while in_flight:
                # Wait for any child to exit. We poll on a timeout so the
                # SIGTERM-set stop_submitting flag is observed promptly.
                sentinels = [p.sentinel for p in in_flight]
                ready = multiprocessing.connection.wait(sentinels, timeout=1.0)

                # Map sentinels back to processes. multiprocessing.connection.wait
                # returns the sentinel objects; we match by identity.
                done_procs = [p for p in list(in_flight) if p.sentinel in ready]
                for proc in done_procs:
                    task_id, _ = in_flight[proc]
                    result = collect_one(proc)

                    if result["status"] == "done":
                        cont_msg = " (needs continuation)" if result.get("needs_continuation") else ""
                        duration = result.get("duration_s") or 0.0
                        print(f"[DAG] {task_id} done ({duration:.1f}s){cont_msg}")
                        if os.environ.get("DAG_VERBOSE") == "1":
                            self._print_node_detail(task_id)
                    else:
                        print(f"[DAG] {task_id} failed: {result.get('error', 'unknown')}")
                        if first_failure is None:
                            first_failure = result
                        if on_failure == "crash":
                            stop_submitting = True

                if self._shutdown_requested:
                    break

                submit_more()

            # Drain any remaining in-flight children after a shutdown signal.
            if in_flight:
                drain_timeout = float(os.environ.get("DAG_DRAIN_TIMEOUT_S", "8"))
                deadline = time.monotonic() + drain_timeout
                while in_flight and time.monotonic() < deadline:
                    remaining = max(0.0, deadline - time.monotonic())
                    sentinels = [p.sentinel for p in in_flight]
                    ready = multiprocessing.connection.wait(sentinels, timeout=remaining)
                    for proc in [p for p in list(in_flight) if p.sentinel in ready]:
                        collect_one(proc)

                # Anyone still alive: SIGTERM, then SIGKILL.
                for proc in list(in_flight):
                    task_id, pipe_r = in_flight[proc]
                    print(f"[DAG] {task_id}: sending SIGTERM to child...")
                    try:
                        proc.terminate()
                        proc.join(timeout=5)
                    except Exception:
                        pass
                    if proc.is_alive():
                        print(f"[DAG] {task_id}: SIGKILL")
                        try:
                            proc.kill()
                            proc.join(timeout=2)
                        except Exception:
                            pass
                    # Synthesize a failure result for shutdown-killed nodes.
                    in_flight.pop(proc, None)
                    now = datetime.now(timezone.utc).isoformat()
                    self._apply_result(task_id, {
                        "task_id": task_id,
                        "status": "failed",
                        "error": "killed during shutdown",
                        "traceback": "",
                        "started_at": self.state[task_id].get("started_at") or now,
                        "finished_at": now,
                        "duration_s": 0.0,
                        "needs_continuation": False,
                        "tracking": {"asset_writers": {}, "asset_versions": {}, "io_records": []},
                    })
                    self.save_state()
                    if first_failure is None:
                        first_failure = self.state[task_id]
        finally:
            # Restore prior signal handler (mostly relevant for tests / repeated runs).
            try:
                signal.signal(signal.SIGTERM, prior_handler)
            except (ValueError, TypeError):
                pass

        # Final state save with overall status
        self.save_state()

        if first_failure is not None:
            failed_id = first_failure.get("id") or first_failure.get("task_id") or "unknown"
            raise RuntimeError(
                f"[DAG] {failed_id} failed: {first_failure.get('error', 'unknown')}"
            )

        return self

    # =========================================================================
    # Serialization
    # =========================================================================

    def _print_node_detail(self, task_id: str) -> None:
        """Print per-node data flow (raw_writes, raw_reads, materializations).

        Called by run() after a successful node when DAG_VERBOSE=1.
        Reads from the tracking module (filled by io.py + delta.py during the node).
        """
        writes = get_assets_by_writer(task_id)
        reads = get_reads_by_task(task_id)

        raw_writes = [w for w in writes if w.startswith("raw/") or "/raw/" in w]
        materializations = []
        for w in writes:
            if w.startswith("subsets/"):
                name = w.replace("subsets/", "")
                vi = get_asset_version(w)
                if vi:
                    materializations.append(f"{name} (v{vi['version']})")
                else:
                    materializations.append(name)
        raw_reads = [r for r in reads if r.startswith("raw/") or "/raw/" in r]

        for label, vals in (
            ("raw_writes", raw_writes),
            ("raw_reads", raw_reads),
            ("materializations", materializations),
        ):
            if vals:
                print(f"      {label + ':':<18}{', '.join(vals)}")

    def _overall_status(self) -> str:
        statuses = [n["status"] for n in self.state.values()]
        if "failed" in statuses:
            return "failed"
        if "running" in statuses:
            return "running"
        if all(s in ("done", "skipped") for s in statuses):
            return "needs_continuation" if self._needs_continuation else "done"
        return "running"

    def to_json(self) -> dict:
        """Build the run.json payload from current state + tracking data."""
        nodes_with_io = []
        for node_state in self.state.values():
            task_id = node_state["id"]
            writes = get_assets_by_writer(task_id)
            reads = get_reads_by_task(task_id)

            raw_writes = [w for w in writes if w.startswith("raw/") or "/raw/" in w]
            materializations = []
            for w in writes:
                if w.startswith("subsets/"):
                    name = w.replace("subsets/", "")
                    vi = get_asset_version(w)
                    if vi:
                        materializations.append({"name": name, **vi})
                    else:
                        materializations.append({"name": name})
            raw_reads = [r for r in reads if r.startswith("raw/") or "/raw/" in r]
            subsets_reads = [
                r.replace("subsets/", "") for r in reads if r.startswith("subsets/")
            ]

            # Merge tracking-derived fields with whatever is in node_state
            merged = {**node_state}
            if raw_writes or not merged.get("raw_writes"):
                merged["raw_writes"] = raw_writes or merged.get("raw_writes", [])
            if raw_reads or not merged.get("raw_reads"):
                merged["raw_reads"] = raw_reads or merged.get("raw_reads", [])
            if subsets_reads or not merged.get("subsets_reads"):
                merged["subsets_reads"] = subsets_reads or merged.get("subsets_reads", [])
            if materializations or not merged.get("materializations"):
                merged["materializations"] = materializations or merged.get("materializations", [])
            nodes_with_io.append(merged)

        return {
            "run_id": os.environ.get("RUN_ID", "unknown"),
            "connector": os.environ.get("CONNECTOR_NAME") or Path.cwd().name,
            "status": self._overall_status(),
            "topology_hash": self.topology_hash,
            "started_at": min(
                (n.get("started_at") for n in self.state.values() if n.get("started_at")),
                default=None,
            ),
            "finished_at": max(
                (n.get("finished_at") for n in self.state.values() if n.get("finished_at")),
                default=None,
            ),
            "dag": {
                "nodes": nodes_with_io,
                "edges": [
                    {"from": self._fn_to_id[dep], "to": self._fn_to_id[fn]}
                    for fn, deps in self.nodes.items()
                    for dep in deps
                ],
                "total_duration_s": sum(
                    n.get("duration_s") or 0 for n in self.state.values()
                ),
            },
        }

    def save_state(self):
        """Write run.json to LOG_DIR. Called after each node, can also be called explicitly."""
        log_dir = os.environ.get("LOG_DIR")
        if not log_dir:
            return  # Local dev without runner — skip persistence
        path = Path(log_dir) / "run.json"
        # Preserve invocations array from prior state if present
        existing = _load_run_state(Path(log_dir)) or {}
        payload = self.to_json()
        if "invocations" in existing:
            payload["invocations"] = existing["invocations"]
        if "git_hash" in existing:
            payload["git_hash"] = existing["git_hash"]
        _atomic_write_json(path, payload)


# =============================================================================
# Node loading
# =============================================================================

def load_nodes(nodes_dir: Path | str | None = None) -> DAG:
    """Discover all node files in `nodes_dir` and assemble their NODES dicts."""
    if nodes_dir is None:
        nodes_dir = Path.cwd() / "src" / "nodes"
    elif isinstance(nodes_dir, str):
        nodes_dir = Path(nodes_dir)

    print(f"Loading nodes from: {nodes_dir}")

    all_nodes: dict[Callable, list[Callable]] = {}

    if not nodes_dir.exists():
        print(f"Warning: nodes directory not found: {nodes_dir}")
        return DAG(all_nodes)

    top_level = sorted(nodes_dir.glob("*.py"))
    nested = sorted(
        f for f in nodes_dir.glob("*/*.py")
        if f.parent.name != "__pycache__"
    )
    node_files = top_level + nested

    for node_file in node_files:
        if node_file.name.startswith("_"):
            continue

        rel = node_file.relative_to(nodes_dir).with_suffix("")
        module_name = "nodes." + ".".join(rel.parts)

        try:
            if module_name in sys.modules:
                module = sys.modules[module_name]
            else:
                spec = importlib.util.spec_from_file_location(module_name, node_file)
                if spec is None or spec.loader is None:
                    print(f"Warning: Could not load spec for {node_file}")
                    continue
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)

            if hasattr(module, "NODES"):
                nodes_dict = getattr(module, "NODES")
                if isinstance(nodes_dict, dict):
                    for fn, deps in nodes_dict.items():
                        all_nodes[fn] = deps

        except Exception as e:
            print(f"Error loading {node_file.name}: {e}")
            raise

    print(f"Loaded {len(all_nodes)} nodes")
    return DAG(all_nodes)
