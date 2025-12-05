"""
Reachability Analysis Implementation (BFS)

Used to find disconnected nodes and validate workflow structure.

Time Complexity: O(V + E)
Space Complexity: O(V)
"""

from collections import deque
from typing import Any, Dict, List, Optional, Set, Union


def find_reachable_nodes(
    nodes: List[Dict[str, Any]], edges: List[Dict[str, str]], start_id: str
) -> Set[str]:
    """
    Find all nodes reachable from the start node using BFS.

    Args:
        nodes: List of node dictionaries with 'id' field
        edges: List of edge dictionaries with 'source' and 'target' fields
        start_node_id: ID of the starting node
    Returns:
        Set of reachable node IDs
    Example:
        >>> nodes = [{'id': 'start'}, {'id': 'a'}, {'id': 'b'}, {'id': 'orphan'}]
        >>> edges = [{'source': 'start', 'target': 'a'}, {'source': 'a', 'target': 'b'}]
        >>> find_reachable_nodes(nodes, edges, 'start')
        {'start', 'a', 'b'}
    """
    # Build adjacency list
    graph = {node["id"]: [] for node in nodes}
    for edge in edges:
        graph[edge["source"]].append(edge["target"])

    visited = set()
    queue = deque([start_id])

    while queue:
        current = queue.popleft()

        if current in visited:
            continue

        visited.add(current)

        for neighbor in graph[current]:
            if neighbor not in visited:
                queue.append(neighbor)

    return visited


def find_unreachable_nodes(
    nodes: List[Dict[str, Any]], edges: List[Dict[str, str]], start_id: str
) -> Set[str]:
    """
    Find all nodes that are not reachable from the start node.

    Args:
        nodes: List of node dictionaries with 'id' field
        edges: List of edge dictionaries with 'source' and 'target' fields
        start_node_id: ID of the starting node
    Returns:
        Set of unreachable node IDs
    Example:
        >>> nodes = [{'id': 'start'}, {'id': 'a'}, {'id': 'b'}, {'id': 'orphan'}]
        >>> edges = [{'source': 'start', 'target': 'a'}, {'source': 'a', 'target': 'b'}]
        >>> find_unreachable_nodes(nodes, edges, 'start')
        {'orphan'}
    """
    reachable = find_reachable_nodes(nodes, edges, start_id)
    all_node_ids = {node["id"] for node in nodes}
    unreachable = all_node_ids - reachable
    return unreachable


def are_nodes_connected(
    nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]], node_a: str, node_b: str
) -> bool:
    """
    Check if there's a path from node_a to node_b.

    Args:
        nodes: List of node dictionaries
        edges: List of edge dictionaries
        node_a: Source node ID
        node_b: Target node ID

    Returns:
        True if path exists, False otherwise
    """
    reachable = find_reachable_nodes(nodes, edges, node_a)
    return node_b in reachable


def shortest_path(
    nodes: List[Dict[str, Any]],
    edges: List[Dict[str, str]],
    start_id: str,
    end_id: str,
) -> List[str]:
    """
    Find the shortest path from start_node_id to end_node_id using BFS.

    Args:
        nodes: List of node dictionaries with 'id' field
        edges: List of edge dictionaries with 'source' and 'target' fields
        start_node_id: ID of the starting node
        end_node_id: ID of the target nodeq
    Returns:
        List of node IDs representing the shortest path, or empty list if no path exists
    Example:
        >>> nodes = [{'id': 'start'}, {'id': 'a'}, {'id': 'b'}, {'id': 'c'}]
        >>> edges = [{'source': 'start', 'target': 'a'}, {'source': 'a', 'target': 'b'}, {'source': 'start', 'target': 'c'}, {'source': 'c', 'target': 'b'}]
        >>> shortest_path(nodes, edges, 'start', 'b')
        ['start', 'a', 'b']
    """
    # Build adjacency list
    graph = {node["id"]: [] for node in nodes}
    for edge in edges:
        graph[edge["source"]].append(edge["target"])

    queue = deque([start_id])
    visited = set()
    parent = {start_id: None}

    while queue:
        current = queue.popleft()

        if current == end_id:
            # Reconstruct path
            path = []
            node = end_id
            while node is not None:
                path.append(node)
                node = parent[node]
            return path[::-1]

        for neighbor in graph[current]:
            if neighbor not in visited:
                visited.add(neighbor)
                parent[neighbor] = current  # type: ignore
                queue.append(neighbor)

    return []  # No path found


def find_nodes_reaching(
    nodes: List[Dict[str, Any]], edges: List[Dict[str, str]], target_id: str
) -> Set[str]:
    """
    Find all nodes that can reach the target node using reverse BFS.

    Args:
        nodes: List of node dictionaries with 'id' field
        edges: List of edge dictionaries with 'source' and 'target' fields
        target_node_id: ID of the target node
    Returns:
        Set of node IDs that can reach the target node
    Example:
        >>> nodes = [{'id': 'start'}, {'id': 'a'}, {'id': 'b'}, {'id': 'orphan'}]
        >>> edges = [{'source': 'start', 'target': 'a'}, {'source': 'a', 'target': 'b'}]
        >>> find_nodes_reaching(nodes, edges, 'b')
        {'start', 'a'}
    """
    # Build reverse adjacency list
    reverse_graph = {node["id"]: [] for node in nodes}
    for edge in edges:
        reverse_graph[edge["target"]].append(edge["source"])

    visited = set()
    queue = deque([target_id])

    while queue:
        current = queue.popleft()

        if current in visited:
            continue

        visited.add(current)

        for neighbor in reverse_graph[current]:
            if neighbor not in visited:
                queue.append(neighbor)

    visited.remove(target_id)  # Exclude the target node itself
    return visited


# example test :
if __name__ == "__main__":
    graph_nodes = [
        {"id": "start"},
        {"id": "a"},
        {"id": "b"},
        {"id": "c"},
        {"id": "orphan"},
    ]
    graph_edges = [
        {"source": "start", "target": "a"},
        {"source": "a", "target": "b"},
        {"source": "a", "target": "c"},
    ]

    print(
        "Reachable from 'start':",
        find_reachable_nodes(graph_nodes, graph_edges, "start"),
    )
    print(
        "Unreachable from 'start':",
        find_unreachable_nodes(graph_nodes, graph_edges, "start"),
    )
    print(
        "Are 'start' and 'a' connected?:",
        are_nodes_connected(graph_nodes, graph_edges, "start", "a"),
    )
    print(
        "Shortest path from 'start' to 'a':",
        shortest_path(graph_nodes, graph_edges, "start", "a"),
    )
    print(
        "Nodes reaching 'b':",
        find_nodes_reaching(graph_nodes, graph_edges, "b"),
    )
