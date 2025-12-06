"""
Test to verify Join Node output in workflow execution
"""

import json

from workflow.engine.executor import WorkflowExecutor


def test_join_output():
    """Test join node output by running workflow and inspecting context."""

    # Load workflow
    with open("workflow_advanced_test.json", "r") as f:
        workflow_data = json.load(f)

    print("\n" + "=" * 60)
    print("TESTING JOIN NODE OUTPUT")
    print("=" * 60)

    # Execute workflow
    executor = WorkflowExecutor(
        run_id="test-run-" + workflow_data["id"], workflow_definition=workflow_data
    )
    result = executor.execute()

    # Get context
    context = executor.coordinator.get_context()

    # Get join node output
    join_output = context.get_step("join_fork_results")

    print("\n[1] Join Node Configuration:")
    join_node = next(
        n for n in workflow_data["nodes"] if n["id"] == "join_fork_results"
    )
    print(json.dumps(join_node, indent=2))

    print("\n[2] Join Node Output:")
    print(json.dumps(join_output, indent=2, default=str))

    if join_output:
        outputs = join_output.get("outputs", {})
        print("\n[3] Join Result Analysis:")
        print(f"   Status: {outputs.get('status', 'unknown')}")
        print(f"   Source: {outputs.get('source', 'unknown')}")
        print(f"   Strategy: {outputs.get('strategy', 'unknown')}")
        print(f"   Count: {outputs.get('count', 0)}")

        aggregated = outputs.get("aggregated", {})
        print(f"\n[4] Aggregated Results ({len(aggregated)} nodes):")
        for node_id, node_output in aggregated.items():
            print(f"\n   Node: {node_id}")
            print(
                f"   Output: {json.dumps(node_output, indent=6, default=str)[:200]}..."
            )

    # Get fork node output for comparison
    fork_output = context.get_step("fork_parallel_operations")
    print("\n[5] Fork Node Output (for comparison):")
    if fork_output:
        outputs = fork_output.get("outputs", {})
        print(f"   Type: {outputs.get('type', 'unknown')}")
        print(f"   Total Paths: {outputs.get('total_paths', 0)}")
        print(f"   Paths Executed: {outputs.get('paths_executed', 0)}")

        paths = outputs.get("paths", {})
        print(f"\n   Paths Details:")
        for path_id, path_data in paths.items():
            print(f"      - {path_id}:")
            print(f"        Condition Met: {path_data.get('condition_met', False)}")
            print(f"        Status: {path_data.get('status', 'unknown')}")
            nodes = path_data.get("nodes", {})
            print(f"        Nodes: {len(nodes)} nodes")

    # Verify join worked correctly
    print("\n[6] Verification:")
    if join_output and join_output.get("outputs", {}).get("status") == "completed":
        print("   [+] Join node executed successfully")
        print("   [+] Results aggregated from fork")
        aggregated = join_output.get("outputs", {}).get("aggregated", {})
        if aggregated:
            print(f"   [+] Aggregated {len(aggregated)} node outputs")
        else:
            print("   [!] No aggregated results found")
    else:
        print("   [X] Join node did not complete successfully")

    print("\n" + "=" * 60)
    print("TEST COMPLETED")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    test_join_output()
