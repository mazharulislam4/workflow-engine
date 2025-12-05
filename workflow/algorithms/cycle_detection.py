"""
Cycle Detection Implementation (DFS with Color Marking)

Used to validate that workflow DAG has no cycles.
Prevents infinite loops in workflow execution.

Time Complexity: O(V + E)
Space Complexity: O(V)
"""

from typing import Any, Dict, List, Optional, Tuple

# Color states for DFS
WHITE = 0  # unvisited
GRAY = 1  # visiting
BLACK = 2  # visited


def has_cycle(
    nodes: List[Dict[str, Any]], edges: List[Dict[str, str]]
) -> Tuple[bool, Optional[List[str]]]:
    """
    Detect cycles in a directed graph using DFS with color marking.

    Args:
        nodes: List of node dictionaries with 'id' field
        edges: List of edge dictionaries with 'source' and 'target' fields
    Returns:
        True if cycle exists, False otherwise. If a cycle exists, also returns the list of node IDs forming the cycle.

        Example:
        >>> nodes = [{'id': 'a'}, {'id': 'b'}, {'id': 'c'}]
        >>> edges = [{'source': 'a', 'target': 'b'}, {'source': 'b', 'target': 'c'}, {'source': 'c', 'target': 'a'}]
        >>> has_cycle(nodes, edges)
        True, ['a', 'b', 'c', 'a']
    """

    graph = {node["id"]: [] for node in nodes}
    for edge in edges:
        graph[edge["source"]].append(edge["target"])

    color = {node["id"]: WHITE for node in nodes}
    parent = {node["id"]: None for node in nodes}

    def dfs(node_id: str) -> Optional[List[str]]:
        color[node_id] = GRAY

        for neighbor in graph[node_id]:
            if color[neighbor] == WHITE:
                parent[neighbor] = node_id  # type: ignore
                cycle_path = dfs(neighbor)
                if cycle_path:
                    return cycle_path
            elif color[neighbor] == GRAY:
                # Cycle detected, reconstruct the cycle path
                cycle = [neighbor]
                current = node_id
                while current != neighbor:
                    cycle.append(current)
                    current = parent[current]
                cycle.append(neighbor)
                cycle.reverse()
                return cycle

        color[node_id] = BLACK
        return None

    for node in nodes:
        if color[node["id"]] == WHITE:
            cycle_path = dfs(node["id"])
            if cycle_path:
                return True, cycle_path

    return False, None


def find_cycle_path(
    nodes: List[Dict[str, Any]], edges: List[Dict[str, str]]
) -> Optional[List[str]]:
    """
    Find the actual cycle path for detailed error messages.

    Args:
        nodes: List of node dictionaries
        edges: List of edge dictionaries

    Returns:
        List of node IDs forming the cycle, or None if no cycle

    Example:
        >>> nodes = [{'id': 'a'}, {'id': 'b'}, {'id': 'c'}]
        >>> edges = [{'source': 'a', 'target': 'b'}, {'source': 'b', 'target': 'c'}, {'source': 'c', 'target': 'a'}]
        >>> find_cycle_path(nodes, edges)
        ['a', 'b', 'c', 'a']
    """
    has_cycle_flag, cycle_path = has_cycle(nodes, edges)
    if has_cycle_flag:
        return cycle_path
    return None


def strongly_connected_components(
    nodes: List[Dict[str, Any]], edges: List[Dict[str, str]]
) -> List[List[str]]:
    """
    Find strongly connected components (SCCs) in the graph using Kosaraju's algorithm.

    Args:
        nodes: List of node dictionaries with 'id' field
        edges: List of edge dictionaries with 'source' and 'target' fields
    Returns:
        List of SCCs, each represented as a list of node IDs
    """
    graph = {node["id"]: [] for node in nodes}
    reverse_graph = {node["id"]: [] for node in nodes}
    for edge in edges:
        graph[edge["source"]].append(edge["target"])
        reverse_graph[edge["target"]].append(edge["source"])

    visited = set()
    finish_stack = []

    def dfs_first_pass(node_id: str):
        visited.add(node_id)
        for neighbor in graph[node_id]:
            if neighbor not in visited:
                dfs_first_pass(neighbor)
        finish_stack.append(node_id)

    for node in nodes:
        if node["id"] not in visited:
            dfs_first_pass(node["id"])

    visited.clear()
    sccs = []

    def dfs_second_pass(node_id: str, current_scc: List[str]):
        visited.add(node_id)
        current_scc.append(node_id)
        for neighbor in reverse_graph[node_id]:
            if neighbor not in visited:
                dfs_second_pass(neighbor, current_scc)

    while finish_stack:
        node_id = finish_stack.pop()
        if node_id not in visited:
            current_scc = []
            dfs_second_pass(node_id, current_scc)
            sccs.append(current_scc)

    return sccs


# example test :
if __name__ == "__main__":
    graph_nodes = [
        {"id": "a"},
        {"id": "b"},
        {"id": "c"},
        {"id": "d"},
    ]
    graph_edges = [
        {"source": "a", "target": "b"},
        {"source": "b", "target": "c"},
        {"source": "c", "target": "a"},  # Cycle here
        {"source": "b", "target": "d"},
    ]
    cycle_exists, cycle_path = has_cycle(graph_nodes, graph_edges)
    print("Cycle Exists:", cycle_exists)
    if cycle_exists:
        print("Cycle Path:", cycle_path)

    sccs = strongly_connected_components(graph_nodes, graph_edges)
    print("Strongly Connected Components:", sccs)
