"""
Print workflow context and join data in JSON format only
"""

import json
import logging

from workflow.engine.executor import WorkflowExecutor

# Suppress all logging
logging.disable(logging.CRITICAL)


def main():
    with open("workflow_advanced_test.json", "r") as f:
        workflow_data = json.load(f)

    executor = WorkflowExecutor(
        run_id="test-run-" + workflow_data["id"], workflow_definition=workflow_data
    )
    executor.execute()

    context = executor.coordinator.get_context()

    # Get full context state
    full_context = context.state

    # Get join node output
    join_output = context.get_step("join_fork_results")

    output = {"workflow_context": full_context, "join_node_data": join_output}

    print(json.dumps(output, indent=2, default=str))


if __name__ == "__main__":
    main()
