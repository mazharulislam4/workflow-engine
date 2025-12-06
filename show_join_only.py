import json
import logging
import sys
from workflow.engine.executor import WorkflowExecutor

logging.disable(logging.CRITICAL)

with open("workflow_advanced_test.json", "r") as f:
    workflow_data = json.load(f)

executor = WorkflowExecutor(
    run_id="test-run-" + workflow_data["id"],
    workflow_definition=workflow_data
)
executor.execute()

context = executor.coordinator.get_context()
join_output = context.get_step("join_fork_results")

sys.stdout.write(json.dumps(join_output, indent=2, default=str))
