"""
Examples demonstrating topological sort algorithms for workflow execution
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from workflow.algorithms.topological_sort import (
    compute_dependency_levels,
    get_parallel_levels,
    group_nodes_by_level,
    topological_sort,
)


def example_simple_workflow():
    """Example 1: Simple linear workflow"""
    print("\n" + "=" * 70)
    print("Example 1: Simple Linear Workflow")
    print("=" * 70)

    nodes = [
        {"id": "start", "type": "start"},
        {"id": "fetch_data", "type": "http"},
        {"id": "transform", "type": "action"},
        {"id": "save", "type": "action"},
        {"id": "end", "type": "end"},
    ]

    edges = [
        {"source": "start", "target": "fetch_data"},
        {"source": "fetch_data", "target": "transform"},
        {"source": "transform", "target": "save"},
        {"source": "save", "target": "end"},
    ]

    print("\nWorkflow: start → fetch_data → transform → save → end")

    # Topological sort
    order = topological_sort(nodes, edges)
    print(f"\nExecution Order: {' → '.join(order)}")

    # Parallel levels
    levels = get_parallel_levels(nodes, edges)
    print(f"\nParallel Levels:")
    for i, level in enumerate(levels):
        print(f"  Level {i}: {level}")

    # Dependency levels
    dep_levels = compute_dependency_levels(nodes, edges)
    print(f"\nDependency Levels:")
    for node_id, level in sorted(dep_levels.items(), key=lambda x: (x[1], x[0])):
        print(f"  {node_id}: Level {level}")


def example_parallel_api_calls():
    """Example 2: Parallel API calls that join"""
    print("\n" + "=" * 70)
    print("Example 2: Parallel API Calls with Join")
    print("=" * 70)

    nodes = [
        {"id": "start", "type": "start"},
        {"id": "fetch_user", "type": "http"},
        {"id": "fetch_orders", "type": "http"},
        {"id": "fetch_inventory", "type": "http"},
        {"id": "join_data", "type": "action"},
        {"id": "process", "type": "action"},
        {"id": "end", "type": "end"},
    ]

    edges = [
        {"source": "start", "target": "fetch_user"},
        {"source": "start", "target": "fetch_orders"},
        {"source": "start", "target": "fetch_inventory"},
        {"source": "fetch_user", "target": "join_data"},
        {"source": "fetch_orders", "target": "join_data"},
        {"source": "fetch_inventory", "target": "join_data"},
        {"source": "join_data", "target": "process"},
        {"source": "process", "target": "end"},
    ]

    print("\nWorkflow:")
    print("           ┌→ fetch_user ─┐")
    print("  start ──→├→ fetch_orders ──→ join_data → process → end")
    print("           └→ fetch_inventory ─┘")

    # Parallel levels
    levels = get_parallel_levels(nodes, edges)
    print(f"\nParallel Levels:")
    for i, level in enumerate(levels):
        print(f"  Level {i}: {level}")
        if i == 1:
            print(f"           ↑ These {len(level)} API calls can run in parallel!")

    # Dependency levels
    dep_levels = compute_dependency_levels(nodes, edges)
    print(f"\nDependency Levels:")
    for node_id, level in sorted(dep_levels.items(), key=lambda x: (x[1], x[0])):
        print(f"  {node_id}: Level {level}")

    # Grouped by level
    grouped = group_nodes_by_level(nodes, dep_levels)
    print(f"\nGrouped Nodes (for parallel execution):")
    for i, level_nodes in enumerate(grouped):
        node_ids = [n["id"] for n in level_nodes]
        print(f"  Level {i}: {node_ids} ({len(level_nodes)} nodes)")


def example_complex_workflow():
    """Example 3: Complex workflow with multiple branches"""
    print("\n" + "=" * 70)
    print("Example 3: Complex E-commerce Order Processing")
    print("=" * 70)

    nodes = [
        {"id": "start", "type": "start"},
        {"id": "validate_order", "type": "action"},
        {"id": "check_inventory", "type": "http"},
        {"id": "check_payment", "type": "http"},
        {"id": "reserve_items", "type": "action"},
        {"id": "charge_payment", "type": "action"},
        {"id": "create_shipment", "type": "action"},
        {"id": "send_confirmation", "type": "action"},
        {"id": "update_analytics", "type": "action"},
        {"id": "end", "type": "end"},
    ]

    edges = [
        {"source": "start", "target": "validate_order"},
        # Parallel checks
        {"source": "validate_order", "target": "check_inventory"},
        {"source": "validate_order", "target": "check_payment"},
        # After checks
        {"source": "check_inventory", "target": "reserve_items"},
        {"source": "check_payment", "target": "charge_payment"},
        # After reservations
        {"source": "reserve_items", "target": "create_shipment"},
        {"source": "charge_payment", "target": "create_shipment"},
        # Final steps
        {"source": "create_shipment", "target": "send_confirmation"},
        {"source": "create_shipment", "target": "update_analytics"},
        {"source": "send_confirmation", "target": "end"},
        {"source": "update_analytics", "target": "end"},
    ]

    print("\nWorkflow:")
    print("  start → validate_order ─┬→ check_inventory → reserve_items ─┐")
    print(
        "                           └→ check_payment → charge_payment ──┴→ create_shipment"
    )
    print("                                                                        │")
    print(
        "                                                   ┌────────────────────┴─────────┐"
    )
    print(
        "                                                   ↓                              ↓"
    )
    print(
        "                                            send_confirmation              update_analytics"
    )
    print(
        "                                                   │                              │"
    )
    print(
        "                                                   └──────────→ end ←─────────────┘"
    )

    # Topological sort
    order = topological_sort(nodes, edges)
    print(f"\nOne Valid Execution Order:")
    print(f"  {' → '.join(order)}")

    # Parallel levels
    levels = get_parallel_levels(nodes, edges)
    print(f"\nParallel Levels ({len(levels)} levels total):")
    for i, level in enumerate(levels):
        print(f"  Level {i}: {level}")

    # Dependency levels with grouping
    dep_levels = compute_dependency_levels(nodes, edges)
    grouped = group_nodes_by_level(nodes, dep_levels)

    print(f"\nExecution Plan (Grouped by Dependency Level):")
    for i, level_nodes in enumerate(grouped):
        node_ids = [n["id"] for n in level_nodes]
        types = [n["type"] for n in level_nodes]

        if len(level_nodes) == 1:
            print(f"  Level {i}: {node_ids[0]} ({types[0]})")
        else:
            print(f"  Level {i}: {len(level_nodes)} parallel tasks")
            for node in level_nodes:
                print(f"           - {node['id']} ({node['type']})")


def example_diamond_pattern():
    """Example 4: Diamond dependency pattern"""
    print("\n" + "=" * 70)
    print("Example 4: Diamond Dependency Pattern")
    print("=" * 70)

    nodes = [
        {"id": "start", "type": "start"},
        {"id": "branch_a", "type": "action"},
        {"id": "branch_b", "type": "action"},
        {"id": "join", "type": "action"},
        {"id": "end", "type": "end"},
    ]

    edges = [
        {"source": "start", "target": "branch_a"},
        {"source": "start", "target": "branch_b"},
        {"source": "branch_a", "target": "join"},
        {"source": "branch_b", "target": "join"},
        {"source": "join", "target": "end"},
    ]

    print("\nWorkflow (Diamond Pattern):")
    print("         start")
    print("        ↙    ↘")
    print("  branch_a  branch_b")
    print("        ↘    ↙")
    print("         join")
    print("           ↓")
    print("          end")

    # Dependency levels
    dep_levels = compute_dependency_levels(nodes, edges)
    print(f"\nDependency Levels:")
    for node_id, level in sorted(dep_levels.items(), key=lambda x: (x[1], x[0])):
        print(f"  {node_id}: Level {level}")

    print("\nExecution Strategy:")
    print("  - Level 0: Execute 'start'")
    print("  - Level 1: Execute 'branch_a' and 'branch_b' in PARALLEL")
    print("  - Level 2: Wait for both branches, then execute 'join'")
    print("  - Level 3: Execute 'end'")


def example_unequal_path_lengths():
    """Example 5: Workflow with different path lengths"""
    print("\n" + "=" * 70)
    print("Example 5: Unequal Path Lengths")
    print("=" * 70)

    nodes = [
        {"id": "start", "type": "start"},
        {"id": "fast_path", "type": "action"},
        {"id": "slow_step1", "type": "action"},
        {"id": "slow_step2", "type": "action"},
        {"id": "slow_step3", "type": "action"},
        {"id": "join", "type": "action"},
        {"id": "end", "type": "end"},
    ]

    edges = [
        {"source": "start", "target": "fast_path"},
        {"source": "start", "target": "slow_step1"},
        {"source": "slow_step1", "target": "slow_step2"},
        {"source": "slow_step2", "target": "slow_step3"},
        {"source": "fast_path", "target": "join"},
        {"source": "slow_step3", "target": "join"},
        {"source": "join", "target": "end"},
    ]

    print("\nWorkflow:")
    print("          start")
    print("        ↙      ↘")
    print("  fast_path   slow_step1")
    print("      ↓             ↓")
    print("      │        slow_step2")
    print("      │             ↓")
    print("      │        slow_step3")
    print("      ↓             ↓")
    print("        join")
    print("          ↓")
    print("         end")

    # Dependency levels
    dep_levels = compute_dependency_levels(nodes, edges)
    print(f"\nDependency Levels:")
    for node_id, level in sorted(dep_levels.items(), key=lambda x: (x[1], x[0])):
        print(f"  {node_id}: Level {level}")

    print(f"\nKey Insight:")
    print(f"  - 'fast_path' completes at level 1")
    print(f"  - 'slow_step3' completes at level 4")
    print(f"  - 'join' must wait for BOTH paths → Level {dep_levels['join']}")
    print(f"  - The SLOWEST path determines when 'join' can execute!")


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("Topological Sort & Dependency Analysis Examples")
    print("=" * 70)

    example_simple_workflow()
    example_parallel_api_calls()
    example_complex_workflow()
    example_diamond_pattern()
    example_unequal_path_lengths()

    print("\n" + "=" * 70)
    print("Examples Complete!")
    print("=" * 70)
