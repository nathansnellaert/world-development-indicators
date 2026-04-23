"""DAG orchestration with run-state persistence and resume.

The DAG class:
- Builds a topological order from `nodes` dict (`{fn: [deps]}`)
- Optionally inherits state from a prior run.json (resume across invocations)
- Runs each node in order, writes run.json after each node
- Marks status as "needs_continuation" if any node returns True (pagination)
- Knows nothing about exit codes, time budgets, or signals — that's the
  supervisor's job (runner.py).

Continuation pattern:
- A node returns True to signal "more work to do, please retrigger me later"
- The orchestrator records this in run.json status field
- Runner.py reads run.json after subprocess exit and translates the status
  to an exit code (0=done, 2=continuation, 1=failed)

Resume pattern:
- LOG_DIR/run.json exists from a prior invocation → load it
- Topology hash matches → inherit "done" status for matching nodes
- Topology hash differs → log warning, ignore prior state, run fresh
"""

import concurrent.futures
import contextvars
import hashlib
import importlib.util
import json
import os
import sys
import tempfile
import threading
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .tracking import (
    set_current_task, get_assets_by_writer, get_reads_by_task,
    get_asset_version, clear_tracking,
)


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


class DAG:
    def __init__(self, nodes: dict[Callable, list[Callable]]):
        self.nodes = nodes
        self.state: dict[str, dict] = {}
        self._fn_to_id: dict[Callable, str] = {}
        self._id_to_fn: dict[str, Callable] = {}
        self._needs_continuation = False
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

    def _run_task(self, fn: Callable) -> dict:
        """Run a single node function inline. Updates self.state in place."""
        task_id = self._fn_to_id[fn]
        task_state = self.state[task_id]
        task_state["status"] = "running"
        task_state["started_at"] = datetime.now(timezone.utc).isoformat()

        set_current_task(task_id)
        try:
            result = fn()
            task_state["status"] = "done"
            # If the node returns True it signals "more work remains" — runner
            # picks up via run.json status="needs_continuation" and exit code 2.
            if result is True:
                task_state["needs_continuation"] = True
                self._needs_continuation = True
        except Exception as e:
            task_state["status"] = "failed"
            task_state["error"] = str(e)
            task_state["traceback"] = traceback.format_exc()
        finally:
            task_state["finished_at"] = datetime.now(timezone.utc).isoformat()
            started = datetime.fromisoformat(task_state["started_at"])
            finished = datetime.fromisoformat(task_state["finished_at"])
            task_state["duration_s"] = (finished - started).total_seconds()
            set_current_task(None)

        return task_state

    def run(self, targets: list[str] | None = None):
        """Execute all nodes in dependency order, writing run.json after each.

        Args:
            targets: Optional list of node names to run (assumes deps already ran).

        Env vars:
            DAG_TARGET: Comma-separated node names to run (overrides `targets`).
            DAG_ON_FAILURE: "crash" (default) or "continue".
            DAG_PARALLELISM: Max concurrent nodes (default 1 = sequential).

        Behavior:
            - On a node returning True: marks needs_continuation, continues running.
            - On node failure with crash mode: drains in-flight tasks, then raises.
            - On node failure with continue mode: raises after all nodes complete.
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

        with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as ex:
            in_flight: dict[concurrent.futures.Future, str] = {}

            def submit_more():
                if stop_submitting:
                    return
                for fn in find_ready():
                    if len(in_flight) >= parallelism:
                        return
                    task_id = self._fn_to_id[fn]
                    # Reserve the slot before submission so the next find_ready()
                    # call doesn't see this node as pending and re-submit it.
                    self.state[task_id]["status"] = "running"
                    print(f"[DAG] Running {task_id}...")
                    ctx = contextvars.copy_context()
                    fut = ex.submit(ctx.run, self._run_task, fn)
                    in_flight[fut] = task_id

            submit_more()

            while in_flight:
                done_futs, _ = concurrent.futures.wait(
                    in_flight.keys(),
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for fut in done_futs:
                    task_id = in_flight.pop(fut)
                    # _run_task captures exceptions internally, but surface any
                    # unexpected ones (e.g. thread/scheduler bugs) loudly.
                    try:
                        fut.result()
                    except Exception as e:
                        st = self.state[task_id]
                        if st.get("status") != "failed":
                            st["status"] = "failed"
                            st["error"] = f"executor: {e}"
                            st["traceback"] = traceback.format_exc()

                    result = self.state[task_id]
                    self.save_state()  # checkpoint after every node

                    if result["status"] == "done":
                        cont_msg = " (needs continuation)" if result.get("needs_continuation") else ""
                        print(f"[DAG] {task_id} done ({result['duration_s']:.1f}s){cont_msg}")
                        if os.environ.get("DAG_VERBOSE") == "1":
                            self._print_node_detail(task_id)
                    else:
                        print(f"[DAG] {task_id} failed: {result.get('error', 'unknown')}")
                        if first_failure is None:
                            first_failure = result
                        if on_failure == "crash":
                            stop_submitting = True

                submit_more()

        # Final state save with overall status
        self.save_state()

        if first_failure is not None:
            raise RuntimeError(
                f"[DAG] {first_failure['id']} failed: {first_failure.get('error', 'unknown')}"
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
