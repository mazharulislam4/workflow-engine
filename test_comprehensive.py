"""
Comprehensive Workflow Test

Tests fork nodes, nested forks, conditions, parallel execution, and error routing.
"""

import json
import logging
import os
import sys

import django

# Configure logging BEFORE Django setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Setup Django settings
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from workflow.engine.executor import WorkflowExecutor

# Load workflow from JSON file
with open("workflow_test_comprehensive.json", "r") as f:
    workflow_definition = json.load(f)

if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("COMPREHENSIVE WORKFLOW TEST")
    print("=" * 80)
    print("\nWorkflow Features:")
    print("  ✓ Fork nodes with multiple paths")
    print("  ✓ Nested fork (fork inside fork)")
    print("  ✓ Parallel execution within paths")
    print("  ✓ Condition nodes with branching")
    print("  ✓ HTTP requests at multiple levels")
    print("  ✓ Success/Error edge routing")
    print("=" * 80)

    executor = WorkflowExecutor(
        run_id="comprehensive-test-001", workflow_definition=workflow_definition
    )

    result = executor.execute()

    print("\n" + "=" * 80)
    print("WORKFLOW RESULT:")
    print("=" * 80)
    print(json.dumps(result, indent=2, default=str))

    print("\n" + "=" * 80)
    print("CONTEXT STATE:")
    print("=" * 80)
    context_state = executor.context.state

    # Print simplified context
    simplified_state = {
        "variables": context_state.get("variables"),
        "steps": {
            k: {
                "status": "executed" if v.get("outputs") else "pending",
                "has_outputs": bool(v.get("outputs")),
                "output_keys": (
                    list(v.get("outputs", {}).keys()) if v.get("outputs") else []
                ),
            }
            for k, v in context_state.get("steps", {}).items()
        },
        "metadata": context_state.get("metadata"),
    }
    print(json.dumps(simplified_state, indent=2, default=str))

    print("\n" + "=" * 80)
    print("EXECUTION SUMMARY:")
    print("=" * 80)
    total_nodes = len(context_state.get("steps", {}))
    executed_nodes = len(
        [s for s in context_state.get("steps", {}).values() if s.get("outputs")]
    )
    print(f"Total Nodes: {total_nodes}")
    print(f"Executed Nodes: {executed_nodes}")
    print(f"Workflow Status: {result.get('status', 'unknown').upper()}")
    print("=" * 80)
