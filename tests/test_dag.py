import pytest
from app import dag


def test_valid_dag_passes():
    tasks = [
        {"key": "a", "depends_on": []},
        {"key": "b", "depends_on": ["a"]},
        {"key": "c", "depends_on": ["a"]},
        {"key": "d", "depends_on": ["b", "c"]},
    ]
    dag.validate_dag(tasks)  # should not raise


def test_cycle_is_rejected():
    tasks = [
        {"key": "a", "depends_on": ["c"]},
        {"key": "b", "depends_on": ["a"]},
        {"key": "c", "depends_on": ["b"]},
    ]
    with pytest.raises(dag.CyclicGraphError):
        dag.validate_dag(tasks)


def test_unknown_dependency_is_rejected():
    tasks = [
        {"key": "a", "depends_on": ["ghost"]},
    ]
    with pytest.raises(dag.UnknownDependencyError):
        dag.validate_dag(tasks)


def test_ready_tasks_respects_dependencies():
    all_keys = {"a", "b", "c"}
    depends_on = {"a": [], "b": ["a"], "c": ["a", "b"]}

    # nothing done yet: only 'a' is ready
    ready = dag.ready_tasks(all_keys, set(), set(), all_keys, depends_on)
    assert ready == {"a"}

    # 'a' done: 'b' becomes ready, 'c' still blocked on 'b'
    ready = dag.ready_tasks(all_keys, {"a"}, set(), {"b", "c"}, depends_on)
    assert ready == {"b"}

    # 'a' and 'b' done: 'c' ready
    ready = dag.ready_tasks(all_keys, {"a", "b"}, set(), {"c"}, depends_on)
    assert ready == {"c"}


def test_downstream_of_failed_task_is_blocked():
    all_keys = {"a", "b", "c"}
    depends_on = {"a": [], "b": ["a"], "c": ["b"]}

    blocked = dag.blocked_by_failure(all_keys, {"a"}, {"b", "c"}, depends_on)
    assert blocked == {"b", "c"}  # both downstream tasks propagate as blocked
