import json
import logging

from workflow.engine.executor import WorkflowExecutor

logging.disable(logging.CRITICAL)

with open("workflow_advanced_test.json", "r") as f:
    workflow_data = json.load(f)

executor = WorkflowExecutor(
    run_id="test-run-" + workflow_data["id"], workflow_definition=workflow_data
)
executor.execute()

context = executor.coordinator.get_context()

join_data = context.get_step("join_fork_results")
fork_data = context.get_step("fork_parallel_operations")

result = {"join_node": join_data, "fork_node": fork_data}

print(json.dumps(result, indent=2, default=str))
