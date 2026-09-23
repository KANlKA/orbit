"""
Pure graph logic for the workflow engine, kept free of DB/HTTP concerns so it's
trivially unit-testable (see tests/test_dag.py).
"""
from collections import defaultdict, deque
from typing import Dict, List, Set


class CyclicGraphError(Exception):
    pass


class UnknownDependencyError(Exception):
    pass


def validate_dag(tasks: List[dict]) -> None:
    """
    tasks: [{"key": "a", "depends_on": ["b", "c"]}, ...]
    Raises CyclicGraphError or UnknownDependencyError if the graph is invalid.
    Uses Kahn's algorithm (BFS topological sort): if we can't consume every
    node, a cycle exists.
    """
    keys = {t["key"] for t in tasks}
    indegree: Dict[str, int] = {t["key"]: 0 for t in tasks}
    adjacency: Dict[str, List[str]] = defaultdict(list)

    for t in tasks:
        for dep in t.get("depends_on", []):
            if dep not in keys:
                raise UnknownDependencyError(
                    f"Task '{t['key']}' depends on unknown task '{dep}'"
                )
            adjacency[dep].append(t["key"])
            indegree[t["key"]] += 1

    queue = deque([k for k, d in indegree.items() if d == 0])
    visited = 0
    while queue:
        node = queue.popleft()
        visited += 1
        for neighbor in adjacency[node]:
            indegree[neighbor] -= 1
            if indegree[neighbor] == 0:
                queue.append(neighbor)

    if visited != len(keys):
        raise CyclicGraphError("Workflow definition contains a cycle")


def ready_tasks(
    all_task_keys: Set[str],
    completed_keys: Set[str],
    failed_keys: Set[str],
    pending_or_queued_keys: Set[str],
    depends_on: Dict[str, List[str]],
) -> Set[str]:
    """
    A task is ready to dispatch when:
      - it hasn't already been queued/run, and
      - every one of its dependencies has completed successfully.
    A task should be SKIPPED (handled by caller) if any dependency is in failed_keys.
    """
    ready = set()
    for key in all_task_keys:
        if key not in pending_or_queued_keys:
            continue
        deps = set(depends_on.get(key, []))
        if deps & failed_keys:
            continue  # caller marks these SKIPPED, not ready
        if deps.issubset(completed_keys):
            ready.add(key)
    return ready


def blocked_by_failure(
    all_task_keys: Set[str],
    failed_keys: Set[str],
    pending_or_queued_keys: Set[str],
    depends_on: Dict[str, List[str]],
) -> Set[str]:
    """Tasks that can never run because an upstream dependency permanently failed."""
    blocked = set()
    changed = True
    effective_failed = set(failed_keys)
    while changed:
        changed = False
        for key in all_task_keys:
            if key in effective_failed or key not in pending_or_queued_keys:
                continue
            deps = set(depends_on.get(key, []))
            if deps & effective_failed:
                blocked.add(key)
                effective_failed.add(key)  # propagate downstream
                changed = True
    return blocked
