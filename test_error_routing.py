"""
Test Error Routing with Success/Error Edges

Tests error handling and routing in workflows.
"""

import json
import logging
import os
import sys

import django

# Configure logging BEFORE Django setup
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Setup Django settings
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from workflow.engine.executor import WorkflowExecutor

# Load workflow from JSON file
with open("workflow_test.json", "r") as f:
    workflow_definition = json.load(f)

if __name__ == "__main__":
    executor = WorkflowExecutor(
        run_id="error-routing-test-001", workflow_definition=workflow_definition
    )

    result = executor.execute()

    print("\n" + "=" * 70)
    print("WORKFLOW RESULT:")
    print("=" * 70)
    print(json.dumps(result, indent=2, default=str))

    print("\n" + "=" * 70)
    print("CONTEXT:")
    print("=" * 70)
    context_state = executor.context.state
    print(json.dumps(context_state, indent=2, default=str))
