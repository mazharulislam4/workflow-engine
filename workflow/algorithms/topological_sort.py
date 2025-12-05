"""
Topological Sort Implementation (Kahn's Algorithm)

Used to determine the execution order of workflow nodes.
Ensures dependencies are executed before their dependents.

Time Complexity: O(V + E)
Space Complexity: O(V)
"""

import logging
from collections import deque
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def topological_sort(
    nodes: List[Dict[str, Any]], edges: List[Dict[str, str]]
) -> List[str]:
    """
    Sort nodes in topological order using Kahn's algorithm.

    Args:
        nodes: List of node dictionaries with 'id' field
        edges: List of edge dictionaries with 'source' and 'target' fields

    Returns:
        List of node IDs in execution order

    Raises:
        ValueError: If graph contains a cycle

    Example:
        >>> nodes = [{'id': 'start'}, {'id': 'a'}, {'id': 'end'}]
        >>> edges = [{'source': 'start', 'target': 'a'}, {'source': 'a', 'target': 'end'}]
        >>> topological_sort(nodes, edges)
        ['start', 'a', 'end']
    """
    graph = {node["id"]: [] for node in nodes}
    in_degree = {node["id"]: 0 for node in nodes}

    for edge in edges:
        src = edge["source"]
        tgt = edge["target"]
        graph[src].append(tgt)
        in_degree[tgt] += 1

    # find all nodes with no incoming edges or no dependencies (in-degree 0)
    queue = deque([node_id for node_id, degree in in_degree.items() if degree == 0])
    result = []

    # Process nodes in topological order
    while queue:
        # Remove node with in-degree 0
        current = queue.popleft()
        result.append(current)
        # Decrease in-degree of neighbors
        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(result) != len(nodes):
        raise ValueError(
            f"Cycle detected in workflow: only {len(result)} of {len(nodes)} nodes processed"
        )

    return result


def get_parallel_levels(
    nodes: List[Dict[str, Any]], edges: List[Dict[str, str]]
) -> List[List[str]]:
    """
    Group nodes into levels where each level can execute in parallel.

    Nodes at the same level have no dependencies on each other.

    Args:
        nodes: List of node dictionaries
        edges: List of edge dictionaries

    Returns:
        List of levels, each level is a list of node IDs

    Example:
        Input:  Start → A → B → End
                       ↓
                       C
        Output: [
            ['start'],
            ['a'],
            ['b', 'c'],  ← Can run in parallel
            ['end']
        ]
    """
    graph = {node["id"]: [] for node in nodes}
    in_degree = {node["id"]: 0 for node in nodes}

    for edge in edges:
        src = edge["source"]
        tgt = edge["target"]
        graph[src].append(tgt)
        in_degree[tgt] += 1

    levels = []
    current_level = [node_id for node_id, degree in in_degree.items() if degree == 0]

    while current_level:
        levels.append(current_level)
        next_level = []

        for node_id in current_level:
            for neighbor in graph[node_id]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    next_level.append(neighbor)

        current_level = next_level
    return levels


def compute_dependency_levels(
    nodes: List[Dict[str, Any]], edges: List[Dict[str, str]]
) -> Dict[str, int]:
    """
    Compute dependency level for each node using BFS.

    Level 0: Nodes with no dependencies (in-degree = 0)
    Level N: Nodes whose all dependencies are in levels 0 to N-1

    Args:
        nodes: List of node dictionaries with 'id' field
        edges: List of edge dictionaries with 'source' and 'target' fields

    Returns:
        Dictionary mapping node_id to its dependency level

    Example:
        nodes = [
            {'id': 'start'},
            {'id': 'api_1'}, {'id': 'api_2'}, {'id': 'api_3'},
            {'id': 'join'},
            {'id': 'end'}
        ]
        edges = [
            {'source': 'start', 'target': 'api_1'},
            {'source': 'start', 'target': 'api_2'},
            {'source': 'start', 'target': 'api_3'},
            {'source': 'api_1', 'target': 'join'},
            {'source': 'api_2', 'target': 'join'},
            {'source': 'api_3', 'target': 'join'},
            {'source': 'join', 'target': 'end'}
        ]

        Result:
        {
            'start': 0,
            'api_1': 1, 'api_2': 1, 'api_3': 1,  # Can run in parallel!
            'join': 2,
            'end': 3
        }
    """

    graph = {node["id"]: [] for node in nodes}
    in_degree = {node["id"]: 0 for node in nodes}

    for edge in edges:
        src = edge["source"]
        tgt = edge["target"]
        graph[src].append(tgt)
        in_degree[tgt] += 1

    queue = deque([node_id for node_id, degree in in_degree.items() if degree == 0])
    levels = {}

    for node_id in queue:
        levels[node_id] = 0

    while queue:
        current = queue.popleft()
        current_level = levels[current]

        for neighbor in graph[current]:
            # Neighbor's level is max of all parent levels + 1
            neighbor_level = current_level + 1

            if neighbor not in levels:
                levels[neighbor] = neighbor_level
            else:
                # If this path gives a higher level, update it
                levels[neighbor] = max(levels[neighbor], neighbor_level)
            # Reduce in-degree
            in_degree[neighbor] -= 1
            # Add to queue when all dependencies are processed
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(levels) != len(nodes):
        raise ValueError(
            f"Cycle detected in workflow: only {len(levels)} of {len(nodes)} nodes processed"
        )
    logger.debug(f"Computed dependency levels for {len(levels)} nodes")
    for node_id, level in sorted(levels.items(), key=lambda x: (x[1], x[0])):
        logger.debug(f"  Level {level}: {node_id}")

    return levels


def group_nodes_by_level(
    nodes: List[Dict[str, Any]], levels: Dict[str, int]
) -> List[List[Dict[str, Any]]]:
    """
    Group nodes by their dependency level.

    Args:
        nodes: List of all nodes
        levels: Dictionary mapping node_id to level

    Returns:
        List of node lists, where index is the level

    Example:
        nodes = [{'id': 'start'}, {'id': 'api_1'}, {'id': 'api_2'}, ...]
        levels = {'start': 0, 'api_1': 1, 'api_2': 1, ...}

        Result:
        [
            [{'id': 'start'}],                               # Level 0
            [{'id': 'api_1'}, {'id': 'api_2'}, {'id': 'api_3'}],  # Level 1
            [{'id': 'join'}],                                # Level 2
            [{'id': 'end'}]                                  # Level 3
        ]
    """
    if not levels:
        return []

    max_level = max(levels.values())
    grouped = [[] for _ in range(max_level + 1)]

    for node in nodes:
        level = levels[node["id"]]
        grouped[level].append(node)

    logger.debug(f"Grouped nodes into {len(grouped)} levels:")
    for level_num, level_nodes in enumerate(grouped):
        node_ids = [n["id"] for n in level_nodes]
        logger.debug(f"  Level {level_num}: {len(level_nodes)} nodes - {node_ids}")

    return grouped


# example test :

if __name__ == "__main__":
    graph_nodes = [
        {"id": "start"},
        {"id": "a"},
        {"id": "b"},
        {"id": "c"},
        {"id": "end"},
    ]
    graph_edges = [
        {"source": "start", "target": "a"},
        {"source": "a", "target": "b"},
        {"source": "b", "target": "end"},
        {"source": "a", "target": "c"},
        {"source": "c", "target": "end"},
    ]
    print("Topological Sort Order:", topological_sort(graph_nodes, graph_edges))
    print("Parallel Levels:", get_parallel_levels(graph_nodes, graph_edges))
