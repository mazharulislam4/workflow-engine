"""
Path Node Executor

Evaluates path condition and determines if the path should execute.

Used in conjunction with fork nodes for conditional branching.
Each path node represents one branch of a fork.
"""

import logging
from typing import Any, Dict

from workflow.executors.base import NodeExecutor
from workflow.executors.registry import register_executor

logger = logging.getLogger(__name__)


@register_executor("path")
class PathNodeExecutor(NodeExecutor):
    """
    Executor for Path nodes.

    Path nodes are used with fork nodes to create conditional branches.
    Each path evaluates its own condition and skips downstream nodes if false.

    Workflow structure:
    {
        "nodes": [
            {
                "id": "fork1",
                "type": "fork"
            },
            {
                "id": "path_a",
                "type": "path",
                "config": {
                    "condition": "{{current.priority}} == 'high'"
                }
            },
            {
                "id": "action_a",
                "type": "action"
            }
        ],
        "edges": [
            {"source": "fork1", "target": "path_a", "type": "fork-branch"},
            {"source": "path_a", "target": "action_a", "type": "normal"}
        ]
    }

    If path condition is false, all downstream nodes are skipped.
    """

    def execute(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the path node by evaluating its condition.

        Args:
            inputs (Dict[str, Any]): The inputs to the node.

        Returns:
            Dict[str, Any]: The outputs of the node.
        """
        config = inputs.get("config", {})
        condition = config.get("condition", False)
        from workflow.utils.node_utils import evaluate_condition

        condition_met = evaluate_condition(condition)

        logger.info(f"Path {self.node_id} condition: {condition} → {condition_met}")

        return {"condition_met": condition_met, "condition": condition}

    def _skip_downstream_nodes(self):
        """
        Skip all downstream nodes if condition not met.

        Marks all nodes downstream of this path node as skipped.
        """
        downstream_nodes = self._find_downstream_nodes()
        for node_id in downstream_nodes:
            self.coordinator.mark_node_skipped(
                node_id,
                reason="path_condition_not_met",
                details={
                    "path_node_id": self.node_id,
                    "message": "Path condition was false, skipping branch",
                },
            )
            logger.info(f"⏭️ Skipping downstream node: {node_id}")

    def _find_downstream_nodes(self):
        """
        Mark all downstream nodes as skipped.

        Finds all nodes reachable from this path node and marks them for skip.
        """

        downstream_nodes = set()
        queue = [self.node_id]
        visited = {self.node_id}

        while queue:
            current = queue.pop(0)

            for edge in self.edges:
                source = edge.get("source", "")
                target = edge.get("target", "")
                if source == current and target not in visited:

                    # skip fork edges (don't cross paths)
                    if edge.get("type") == "fork-branch":
                        continue
                    downstream_nodes.add(target)
                    visited.add(target)
                    queue.append(target)
        return downstream_nodes
