"""
Condition Node Executor

Evaluates conditional expressions for if/else branching.

Uses pure function pattern for testability.
"""

import json
import logging
from typing import Any, Dict

from workflow.executors.base import NodeExecutor
from workflow.executors.registry import register_executor

logger = logging.getLogger(__name__)


@register_executor("condition")
class ConditionNodeExecutor(NodeExecutor):
    """
    Executor for Condition nodes.

    Condition nodes evaluate a boolean expression to determine
    the execution path (if/else) in the workflow.


    """

    def execute(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the condition node.

        Args:
            inputs: Dictionary containing:
                - "expression": The boolean expression to evaluate.

        Returns:
            Dictionary with:
                - "result": Boolean outcome of the condition.
        """
        config = inputs.get("config", {})
        node = inputs.get("node", {})
        node_id = node.get("node_id", "unknown")
        condition_expr = config.get("expression")

        if condition_expr is None:
            raise ValueError(
                "Condition expression is required for ConditionNodeExecutor."
            )

        logger.info(f"🔀 Evaluating condition [{node_id}]: {condition_expr}")
        logger.debug(f"Condition Config:\n{json.dumps(config, indent=2, default=str)}")
        from workflow.utils.node_utils import evaluate_condition

        # Use the secure evaluation method
        result = evaluate_condition(condition_expr)

        logger.info(
            f"{'✅' if result else '❌'} Condition result [{node_id}]: {result} - Taking {'TRUE' if result else 'FALSE'} branch"
        )

        return {"result": result}

    def _post_execution(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Mark nodes on non-taken branch as skipped.

        After evaluating condition, we need to mark nodes on the
        path NOT taken as skipped so they won't execute.

        Args:
            result: Execution result containing 'result' (true/false)
        """
        import json

        connected_edges = self.filter_connected_edges(self.node_id, "condition")
        condition_result = result.get("result", False)

        logger.debug(f"Post-execution routing for condition [{self.node_id}]")
        logger.debug(
            f"Connected edges: {json.dumps(connected_edges, indent=2, default=str)}"
        )

        true_branch_nodes = []
        false_branch_nodes = []

        for edge in connected_edges:
            target_node_id = edge.get("target")
            edge_condition = edge.get("condition")

            if isinstance(edge_condition, bool):
                condition_value = edge_condition
            else:
                condition_value = str(edge_condition).lower() in ["true", "1", "yes"]

            if condition_value in [True, "true", "True", "1", 1, "yes"]:
                true_branch_nodes.append(target_node_id)
            else:
                # default to false branch
                false_branch_nodes.append(target_node_id)

        logger.debug(f"True branch nodes: {true_branch_nodes}")
        logger.debug(f"False branch nodes: {false_branch_nodes}")

        # Mark nodes on non-taken branch as skipped
        if condition_result:
            # condition true , skip false branch nodes
            node_to_skip = false_branch_nodes
            taken_branch = "TRUE"
            active_nodes = true_branch_nodes
        else:
            # condition false , skip true branch nodes
            node_to_skip = true_branch_nodes
            taken_branch = "FALSE"
            active_nodes = false_branch_nodes

        logger.info(
            f"🔀 Branch routing [{self.node_id}]: Taking {taken_branch} branch → {active_nodes}"
        )

        if node_to_skip:
            logger.info(
                f"⏭️  Skipping {taken_branch == 'TRUE' and 'FALSE' or 'TRUE'} branch nodes: {node_to_skip}"
            )

        for node_id in node_to_skip:
            logger.debug(f"Marking node '{node_id}' as skipped (condition not met)")
            self.coordinator.mark_node_skipped(
                node_id,
                reason="condition_not_met",
                details={
                    "taken_branch": taken_branch,
                    "condition_node": self.node_id,
                    "condition_result": condition_result,
                    "message": f"Condition evaluated to {condition_result}, skipping {taken_branch == 'TRUE' and 'FALSE' or 'TRUE'} branch",
                },
            )

            logger.info(
                f"Marking node {node_id} for skip (condition was {condition_result})"
            )

            logger.info(
                f"Condition {self.node_id} evaluated to {condition_result}, took {taken_branch} branch"
            )
        return condition_result
