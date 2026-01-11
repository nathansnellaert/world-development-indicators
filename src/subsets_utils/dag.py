"""Simple DAG execution with task tracking.

Usage:
    from subsets_utils import DAG

    workflow = DAG({
        ingest: [],
        transform: [ingest],
    })

    workflow.run()                        # Run all nodes
    workflow.run(targets=["transform"])   # Run single node (assumes deps ran)

CLI pattern for main.py:
    if __name__ == "__main__":
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("--target", help="Run specific node")
        args = parser.parse_args()
        workflow.run(targets=[args.target] if args.target else None)
"""
import json
import os
import traceback
import multiprocessing
from datetime import datetime
from pathlib import Path
from typing import Callable
from contextvars import ContextVar

# Current task context for IO tracking
_current_task: ContextVar[dict | None] = ContextVar('current_task', default=None)


def track_read(asset: str):
    """Track a read operation. Called by IO functions."""
    task = _current_task.get()
    if task is not None:
        task["reads"].append(asset)


def track_write(asset: str, rows: int = None):
    """Track a write operation. Called by IO functions."""
    task = _current_task.get()
    if task is not None:
        task["writes"].append(asset)
        if rows:
            task["rows_written"] = task.get("rows_written", 0) + rows


def _get_task_id(fn: Callable) -> str:
    """Get unique task ID from function (module.name)."""
    module = fn.__module__
    # Strip 'src.' prefix if present for cleaner IDs
    if module.startswith('src.'):
        module = module[4:]
    return f"{module}.{fn.__name__}"


class DAG:
    def __init__(self, nodes: dict[Callable, list[Callable]]):
        self.nodes = nodes
        self.state = {}
        self._fn_to_id = {}  # Map function to its ID

        # Initialize state for each node
        for fn in nodes:
            task_id = _get_task_id(fn)
            self._fn_to_id[fn] = task_id
            self.state[task_id] = {
                "id": task_id,
                "status": "pending",
                "reads": [],
                "writes": [],
            }

    def _topological_order(self) -> list[Callable]:
        """Return functions in dependency order."""
        in_degree = {fn: len(deps) for fn, deps in self.nodes.items()}
        ready = [fn for fn, deg in in_degree.items() if deg == 0]
        order = []

        while ready:
            fn = ready.pop(0)
            order.append(fn)

            for other_fn, deps in self.nodes.items():
                if fn in deps:
                    in_degree[other_fn] -= 1
                    if in_degree[other_fn] == 0:
                        ready.append(other_fn)

        if len(order) != len(self.nodes):
            raise ValueError("Cycle detected in DAG")

        return order

    def _run_task(self, fn: Callable, isolate: bool = False) -> dict:
        """Run a single task, optionally in subprocess for memory isolation."""
        task_id = self._fn_to_id[fn]
        task_state = self.state[task_id]
        task_state["status"] = "running"
        task_state["started_at"] = datetime.now().isoformat()

        if isolate:
            return self._run_in_subprocess(fn, task_state)
        else:
            return self._run_inline(fn, task_state)

    def _run_inline(self, fn: Callable, task_state: dict) -> dict:
        """Run task in current process."""
        # Set context for IO tracking
        _current_task.set(task_state)

        try:
            fn()
            task_state["status"] = "done"
        except Exception as e:
            task_state["status"] = "failed"
            task_state["error"] = str(e)
            task_state["traceback"] = traceback.format_exc()
        finally:
            task_state["finished_at"] = datetime.now().isoformat()
            started = datetime.fromisoformat(task_state["started_at"])
            finished = datetime.fromisoformat(task_state["finished_at"])
            task_state["duration_s"] = (finished - started).total_seconds()
            _current_task.set(None)

        return task_state

    def _run_in_subprocess(self, fn: Callable, task_state: dict) -> dict:
        """Run task in subprocess for memory isolation."""
        def worker(fn, queue):
            # Re-setup context in subprocess
            local_state = {"reads": [], "writes": [], "rows_written": 0}
            _current_task.set(local_state)

            try:
                fn()
                queue.put({
                    "status": "done",
                    "reads": local_state["reads"],
                    "writes": local_state["writes"],
                    "rows_written": local_state.get("rows_written", 0),
                })
            except Exception as e:
                queue.put({
                    "status": "failed",
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                })

        queue = multiprocessing.Queue()
        proc = multiprocessing.Process(target=worker, args=(fn, queue))
        proc.start()
        proc.join()

        result = queue.get()
        task_state.update(result)
        task_state["finished_at"] = datetime.now().isoformat()
        started = datetime.fromisoformat(task_state["started_at"])
        finished = datetime.fromisoformat(task_state["finished_at"])
        task_state["duration_s"] = (finished - started).total_seconds()

        return task_state

    def run(self, isolate: bool = False, targets: list[str] | None = None):
        """Execute all tasks in dependency order.

        Args:
            isolate: Run each task in subprocess for memory isolation
            targets: Optional list of node names to run (assumes deps already ran)
        """
        order = self._topological_order()

        # Filter to targets if specified
        if targets:
            target_set = set(targets)
            order = [fn for fn in order if self._fn_to_id[fn].split(".")[-2] in target_set]
            if not order:
                # Try matching full ID or function name
                order = [fn for fn in self._topological_order()
                         if self._fn_to_id[fn] in target_set or fn.__name__ in target_set]
            if not order:
                print(f"[DAG] No nodes matched targets: {targets}")
                print(f"[DAG] Available: {[self._fn_to_id[fn] for fn in self.nodes]}")
                return self

        for fn in order:
            task_id = self._fn_to_id[fn]
            deps = self.nodes[fn]

            # Check deps succeeded
            for dep in deps:
                dep_id = self._fn_to_id[dep]
                if self.state[dep_id]["status"] != "done":
                    self.state[task_id]["status"] = "skipped"
                    self.state[task_id]["error"] = f"Dependency {dep_id} failed"
                    continue

            print(f"[DAG] Running {task_id}...")
            result = self._run_task(fn, isolate=isolate)
            self.save_state()  # Implicit checkpoint after each node

            if result["status"] == "done":
                print(f"[DAG] {task_id} done ({result['duration_s']:.1f}s)")
            else:
                print(f"[DAG] {task_id} failed: {result.get('error', 'unknown')}")
                break  # Stop on failure

        return self

    def to_json(self) -> dict:
        """Export DAG structure and execution state."""
        return {
            "nodes": list(self.state.values()),
            "edges": [
                {"from": self._fn_to_id[dep], "to": self._fn_to_id[fn]}
                for fn, deps in self.nodes.items()
                for dep in deps
            ],
            "status": self._overall_status(),
            "total_duration_s": sum(
                n.get("duration_s", 0) for n in self.state.values()
            ),
        }

    def _overall_status(self) -> str:
        statuses = [n["status"] for n in self.state.values()]
        if "failed" in statuses:
            return "failed"
        if "running" in statuses:
            return "running"
        if all(s == "done" for s in statuses):
            return "done"
        return "pending"

    def save_state(self):
        """Save execution state to LOG_DIR. Called after each node, can also be called explicitly."""
        log_dir = os.environ.get('LOG_DIR')
        if not log_dir:
            return  # No LOG_DIR = local dev without runner, skip
        path = Path(log_dir) / "dag.json"
        path.write_text(json.dumps(self.to_json(), indent=2))


