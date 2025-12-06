"""
Simple Fork Test - Basic fork with 2 paths
"""

import json
import logging
import os

import django

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from workflow.engine.executor import WorkflowExecutor

workflow = {
    "id": "advanced-fork-test",
    "name": "Advanced Fork Test with Success/Fail Paths",
    "version": "1.0.0",
    "variables": {
        "api1": "https://jsonplaceholder.typicode.com/posts/1",
        "api2": "https://jsonplaceholder.typicode.com/posts/2",
        "api3": "https://jsonplaceholder.typicode.com/posts/3",
        "api4": "https://jsonplaceholder.typicode.com/users/1",
        "api5": "https://jsonplaceholder.typicode.com/users/2",
        "invalid_url": "https://httpstat.us/500",
    },
    "nodes": [
        {"id": "start", "type": "start", "name": "Start", "config": {}},
        # Main Fork
        {
            "id": "fork_main",
            "type": "fork",
            "name": "Main Fork",
            "config": {"execution_mode": "thread"},
        },
        # Path 1 - Success Path with Parallel Nodes
        {
            "id": "path_success",
            "type": "path",
            "name": "Success Path",
            "config": {"condition": True},
        },
        {
            "id": "success_http_1",
            "type": "http_request",
            "name": "Success HTTP 1",
            "config": {
                "method": "GET",
                "url": "{{variables.api1}}",
                "headers": {"Content-Type": "application/json"},
            },
        },
        {
            "id": "success_http_2",
            "type": "http_request",
            "name": "Success HTTP 2",
            "config": {
                "method": "GET",
                "url": "{{variables.api2}}",
                "headers": {"Content-Type": "application/json"},
            },
        },
        {
            "id": "success_http_3",
            "type": "http_request",
            "name": "Success HTTP 3",
            "config": {
                "method": "GET",
                "url": "{{variables.api3}}",
                "headers": {"Content-Type": "application/json"},
            },
        },
        # Path 2 - Fail Path
        {
            "id": "path_fail",
            "type": "path",
            "name": "Fail Path",
            "config": {"condition": True},
        },
        {
            "id": "fail_http",
            "type": "http_request",
            "name": "Fail HTTP",
            "config": {
                "method": "GET",
                "url": "{{variables.invalid_url}}",
                "headers": {"Content-Type": "application/json"},
                "error_handling": {"continue_on_error": True},
            },
        },
        # Path 3 - Nested Fork Inside Fork
        {
            "id": "path_nested",
            "type": "path",
            "name": "Nested Fork Path",
            "config": {"condition": True},
        },
        {
            "id": "fork_nested",
            "type": "fork",
            "name": "Nested Fork",
            "config": {"execution_mode": "thread"},
        },
        # Nested Path 1
        {
            "id": "nested_path_1",
            "type": "path",
            "name": "Nested Path 1",
            "config": {"condition": True},
        },
        {
            "id": "nested_http_1",
            "type": "http_request",
            "name": "Nested HTTP 1",
            "config": {
                "method": "GET",
                "url": "{{variables.api4}}",
                "headers": {"Content-Type": "application/json"},
            },
        },
        # Nested Path 2
        {
            "id": "nested_path_2",
            "type": "path",
            "name": "Nested Path 2",
            "config": {"condition": True},
        },
        {
            "id": "nested_http_2",
            "type": "http_request",
            "name": "Nested HTTP 2",
            "config": {
                "method": "GET",
                "url": "{{variables.api5}}",
                "headers": {"Content-Type": "application/json"},
            },
        },
        # Path 4 - Conditional Path (should be skipped)
        {
            "id": "path_skip",
            "type": "path",
            "name": "Skip Path",
            "config": {"condition": False},
        },
        {
            "id": "skip_http",
            "type": "http_request",
            "name": "Skip HTTP",
            "config": {
                "method": "GET",
                "url": "{{variables.api1}}",
                "headers": {"Content-Type": "application/json"},
            },
        },
        {"id": "end", "type": "end", "name": "End", "config": {}},
    ],
    "edges": [
        {"source": "start", "target": "fork_main"},
        # Main fork branches
        {"source": "fork_main", "target": "path_success", "type": "fork-branch"},
        {"source": "fork_main", "target": "path_fail", "type": "fork-branch"},
        {"source": "fork_main", "target": "path_nested", "type": "fork-branch"},
        {"source": "fork_main", "target": "path_skip", "type": "fork-branch"},
        # Success path - parallel nodes
        {"source": "path_success", "target": "success_http_1"},
        {"source": "path_success", "target": "success_http_2"},
        {"source": "path_success", "target": "success_http_3"},
        # Fail path
        {"source": "path_fail", "target": "fail_http"},
        # Nested fork path
        {"source": "path_nested", "target": "fork_nested"},
        {"source": "fork_nested", "target": "nested_path_1", "type": "fork-branch"},
        {"source": "fork_nested", "target": "nested_path_2", "type": "fork-branch"},
        {"source": "nested_path_1", "target": "nested_http_1"},
        {"source": "nested_path_2", "target": "nested_http_2"},
        # Skip path (condition=false, should not execute)
        {"source": "path_skip", "target": "skip_http"},
        {"source": "fork_main", "target": "end"},
    ],
}

if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("ADVANCED FORK TEST")
    print("=" * 80)
    print("\nTest Features:")
    print("  - Path 1: Success path with 3 parallel HTTP requests")
    print("  - Path 2: Fail path (500 error)")
    print("  - Path 3: Nested fork inside fork")
    print("  - Path 4: Skipped path (condition=false)")
    print("=" * 80 + "\n")

    executor = WorkflowExecutor(
        run_id="advanced-fork-001", workflow_definition=workflow
    )
    result = executor.execute()

    print("\n" + "=" * 80)
    print("WORKFLOW RESULT:")
    print("=" * 80)
    print(json.dumps(result, indent=2, default=str))

    print("\n" + "=" * 80)
    print("COMPLETE CONTEXT DATA:")
    print("=" * 80)
    context_state = executor.context.state
    print(json.dumps(context_state, indent=2, default=str))

    print("\n" + "=" * 80)
    print("EXECUTION SUMMARY:")
    print("=" * 80)

    all_nodes = [n["id"] for n in workflow["nodes"]]
    steps = context_state.get("steps", {})

    executed_nodes = []
    skipped_nodes = []
    failed_nodes = []

    for node_id in all_nodes:
        is_skipped, skip_info = executor.coordinator.is_node_skipped(node_id)

        if is_skipped:
            reason = skip_info.get("reason", "unknown")
            skipped_nodes.append(f"{node_id} ({reason})")
        elif node_id in steps:
            step_data = steps[node_id]
            if "error" in step_data.get("outputs", {}):
                failed_nodes.append(node_id)
            else:
                executed_nodes.append(node_id)

    print(f"\n[SUCCESS] EXECUTED NODES ({len(executed_nodes)}):")
    for node in executed_nodes:
        print(f"  - {node}")

    print(f"\n[FAILED] FAILED NODES ({len(failed_nodes)}):")
    if failed_nodes:
        for node in failed_nodes:
            error_msg = (
                steps[node]["outputs"].get("error", {}).get("message", "Unknown error")
            )
            print(f"  - {node}: {error_msg}")
    else:
        print("  - None")

    print(f"\n[SKIPPED] SKIPPED NODES ({len(skipped_nodes)}):")
    if skipped_nodes:
        for node in skipped_nodes:
            print(f"  - {node}")
    else:
        print("  - None")

    print("\n" + "=" * 80)
    print("FORK EXECUTION DETAILS:")
    print("=" * 80)

    # Main Fork Details
    if "fork_main" in steps:
        fork_output = steps["fork_main"].get("outputs", {})
        print(f"\n📊 Main Fork:")
        print(f"  Total Paths: {fork_output.get('total_paths', 0)}")
        print(f"  Paths Executed: {fork_output.get('paths_executed', 0)}")

        paths = fork_output.get("paths", {})
        for path_id, path_data in paths.items():
            status = "[SUCCESS]" if path_data.get("condition_met") else "[SKIPPED]"
            print(f"\n  Path: {path_id} - {status}")
            nodes_in_path = path_data.get("nodes", {})
            print(f"    Nodes executed: {len(nodes_in_path)}")
            for node_id in nodes_in_path:
                print(f"      - {node_id}")

    # Nested Fork Details
    if "fork_nested" in steps:
        nested_fork_output = steps["fork_nested"].get("outputs", {})
        print(f"\n[NESTED] Nested Fork (Fork inside Fork):")
        print(f"  Total Paths: {nested_fork_output.get('total_paths', 0)}")
        print(f"  Paths Executed: {nested_fork_output.get('paths_executed', 0)}")

        nested_paths = nested_fork_output.get("paths", {})
        for path_id, path_data in nested_paths.items():
            status = "[SUCCESS]" if path_data.get("condition_met") else "[SKIPPED]"
            print(f"\n  Nested Path: {path_id} - {status}")
            nodes_in_path = path_data.get("nodes", {})
            print(f"    Nodes executed: {len(nodes_in_path)}")

    print("\n" + "=" * 80)
    print("TEST COMPLETED!")
    print("=" * 80)
