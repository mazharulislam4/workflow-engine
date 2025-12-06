"""
Test Join Node Executor

Tests the join node aggregation logic with fork results.
"""

import json
import logging

from workflow.engine.context import ContextManager
from workflow.executors.join import JoinNodeExecutor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_join_with_fork_results():
    """Test join node with mock fork results."""

    # Mock workflow context with fork results
    context = ContextManager()

    # Simulate fork output structure
    fork_output = {
        "type": "fork",
        "total_paths": 2,
        "paths_executed": 2,
        "paths": {
            "path_a": {
                "condition_met": True,
                "status": "success",
                "nodes": {
                    "task_a1": {
                        "status": "success",
                        "output": {"result": "Task A1 completed", "data": {"count": 5}},
                    },
                    "task_a2": {
                        "status": "success",
                        "output": {"result": "Task A2 completed", "data": {"count": 3}},
                    },
                },
            },
            "path_b": {
                "condition_met": True,
                "status": "success",
                "nodes": {
                    "task_b1": {
                        "status": "success",
                        "output": {"result": "Task B1 completed", "data": {"count": 7}},
                    }
                },
            },
        },
    }

    # Add fork output to context
    context.set_step("fork1", {}, fork_output, {})

    # Create join executor
    join_node = {
        "id": "join1",
        "type": "join",
        "config": {"source": "fork1", "strategy": "merge"},
    }

    # Mock workflow executor
    class MockWorkflowExecutor:
        def __init__(self):
            self.edges = []
            self.nodes = []

    # Mock coordinator
    class MockCoordinator:
        def __init__(self, context):
            self._context = context
            context._set_workflow_executor(MockWorkflowExecutor())

        def get_context(self):
            return self._context

    coordinator = MockCoordinator(context)

    # Create executor
    executor = JoinNodeExecutor(node=join_node, coordinator=coordinator)

    # Execute join
    result = executor.execute({"config": join_node["config"], "node_id": "join1"})

    print("\n" + "=" * 60)
    print("JOIN NODE TEST RESULTS")
    print("=" * 60)
    print("\nJoin Configuration:")
    print(json.dumps(join_node["config"], indent=2))
    print("\nJoin Result:")
    print(json.dumps(result, indent=2, default=str))

    # Verify results
    assert result["status"] == "completed"
    assert result["source"] == "fork1"
    assert result["strategy"] == "merge"
    assert result["count"] == 2  # 2 paths executed

    # Check aggregated data
    aggregated = result["aggregated"]
    assert "task_a1" in aggregated
    assert "task_a2" in aggregated
    assert "task_b1" in aggregated

    print("\n✓ All assertions passed!")
    print("\nAggregated node outputs:")
    for node_id, output in aggregated.items():
        print(f"  - {node_id}: {output}")


def test_join_strategies():
    """Test different aggregation strategies."""

    context = ContextManager()

    fork_output = {
        "type": "fork",
        "total_paths": 2,
        "paths_executed": 2,
        "paths": {
            "path_a": {
                "condition_met": True,
                "status": "success",
                "nodes": {
                    "task_a1": {
                        "status": "success",
                        "output": {"result": "A1", "value": 10},
                    }
                },
            },
            "path_b": {
                "condition_met": True,
                "status": "success",
                "nodes": {
                    "task_b1": {
                        "status": "success",
                        "output": {"result": "B1", "value": 20},
                    }
                },
            },
        },
    }

    context.set_step("fork1", {}, fork_output, {})

    # Mock workflow executor
    class MockWorkflowExecutor:
        def __init__(self):
            self.edges = []
            self.nodes = []

    class MockCoordinator:
        def __init__(self, context):
            self._context = context
            context._set_workflow_executor(MockWorkflowExecutor())

        def get_context(self):
            return self._context

    coordinator = MockCoordinator(context)

    strategies = ["merge", "list", "first", "count", "outputs"]

    print("\n" + "=" * 60)
    print("TESTING DIFFERENT AGGREGATION STRATEGIES")
    print("=" * 60)

    for strategy in strategies:
        join_node = {
            "id": f"join_{strategy}",
            "type": "join",
            "config": {"source": "fork1", "strategy": strategy},
        }

        executor = JoinNodeExecutor(node=join_node, coordinator=coordinator)
        result = executor.execute(
            {"config": join_node["config"], "node_id": join_node["id"]}
        )

        print(f"\nStrategy: {strategy}")
        print(
            f"Result: {json.dumps(result['aggregated'], indent=2, default=str)[:200]}..."
        )

    print("\n✓ All strategy tests completed!")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("STARTING JOIN NODE TESTS")
    print("=" * 60)

    test_join_with_fork_results()
    test_join_strategies()

    print("\n" + "=" * 60)
    print("ALL TESTS PASSED!")
    print("=" * 60 + "\n")
