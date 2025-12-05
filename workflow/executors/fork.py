"""
Fork Node Executor with Celery and Enhanced Limits

Uses Celery for distributed execution instead of threads.
Supports both per-path and overall fork limits.
"""

import logging
from typing import Any, Dict, List

from workflow.executors.base import NodeExecutor
from workflow.executors.registry import register_executor

logger = logging.getLogger(__name__)


@register_executor("fork")
class ForkNodeExecutor(NodeExecutor):
    """
    Executor for Fork nodes with Celery-based parallel execution.

    Features:
    - Celery tasks for distributed execution (better than threads)
    - Per-path execution limits
    - Overall fork execution limits
    - Nested data structure for organized results

    Configuration:
    {
        "type": "fork",
        "config": {

        }
    }
    """

    def execute(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute fork using Celery or threads.

        Args:
            inputs: Contains 'config' with limits and execution mode

        Returns:
            Nested dict with path results
        """

        config = inputs.get("config", {})
        node = inputs.get("node", {})

        max_workers = 10
        max_nodes_per_path = 50
        max_total_nodes = 200
        execution_mode = config.get("execution_mode", "thread")

        logger.info(
            f"🔀 Executing fork node [{node.get('node_id', 'unknown')}] in {execution_mode} mode"
        )

        # find all path nodes

    def _get_path_nodes(self) -> List[Dict[str, Any]]:
        """Get all path nodes connected to this fork"""
        path_nodes_ids = []
        nodes = self.nodes
        edges = self.edges

        for edge in edges:
            source = edge.get("source", "")
            if source == self.node_id and edge.get("type") == "fork-branch":
                target = edge.get("target", "")
                path_nodes_ids.append(target)

        nodes_dict = {n["id"]: n for n in nodes}

        return [
            nodes_dict[path_id] for path_id in path_nodes_ids if path_id in nodes_dict
        ]

    def _validate_limits(
        self, path_nodes: List[Dict[str, Any]], max_per_path: int, max_total: int
    ):
        """
        Validate execution limits to prevent overload.

        Args:
            path_nodes: Path nodes to execute
            max_per_path: Maximum nodes allowed per path
            max_total: Maximum total nodes across all paths

        Raises:
            ValueError: If limits exceeded
        """
        total_nodes = 0

        for path_node in path_nodes:
            path_id = path_node.get("id", "")
            downstream = self._count_downstream_nodes(path_id, self.edges)

            # Check per-path limit
            if downstream > max_per_path:
                raise ValueError(
                    f"Path node {path_id} exceeds max nodes per path limit: {downstream} > {max_per_path}"
                )
            total_nodes += downstream

        # Check overall limit
        if total_nodes > max_total:
            raise ValueError(
                f"Total downstream nodes {total_nodes} exceed max total nodes limit: {max_total}"
            )

        logger.info(
            f"fork node [{self.node_id}] passed limit validation: {total_nodes} total nodes across {len(path_nodes)} paths"
        )

    def _count_downstream_nodes(self, path_id: str, edges: List[Dict[str, Any]]) -> int:
        """
        Count nodes downstream of a path (BFS).

        Args:
            path_id: Path node ID
            edges: Workflow edges

        Returns:
            Number of downstream nodes
        """
        count = 0
        visited = set()
        queue = [path_id]

        while queue:
            current = queue.pop(0)
            for edge in edges:
                source = edge.get("source", "")
                target = edge.get("target", "")
                if source == current and target not in visited:
                    visited.add(target)
                    count += 1
                    queue.append(target)

        return count
