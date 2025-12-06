"""
Path Node Executor

Evaluates path condition and executes downstream nodes in parallel.

Path node acts as a gateway:
- If condition is TRUE: execute all downstream nodes (in parallel!)
- If condition is FALSE: skip all downstream nodes

Path completes only after all downstream nodes complete.
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict

from workflow.executors.base import NodeExecutor
from workflow.executors.registry import create_executor, register_executor

logger = logging.getLogger(__name__)


@register_executor("path")
class PathNodeExecutor(NodeExecutor):
    """
    Executor for Path nodes with parallel downstream execution.

    Key behavior:
    - Evaluate condition (using templates and context)
    - If TRUE: execute downstream nodes in PARALLEL (using dependency levels)
    - If FALSE: skip all downstream nodes

    Parallel execution:
    - Uses dependency level algorithm (same as WorkflowExecutor)
    - Supports fanout patterns: node1 -> node2, node3 (both parallel)
    - Waits for all nodes at each level before proceeding
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
        node_id = inputs.get("node_id", "unknown")
        condition = config.get("condition", False)

        logger.info(f"Path [{node_id}]: Evaluating condition: {condition}")
        logger.debug(f"Path Config:\n{json.dumps(config, indent=2, default=str)}")

        from workflow.utils.node_utils import evaluate_condition

        condition_met = evaluate_condition(condition)

        logger.info(
            f"{'[OK]' if condition_met else '[FAIL]'} Path Node [{node_id}]: Condition {condition} -> {condition_met}"
        )

        if not condition_met:
            logger.info(
                f"[SKIP]  Path Node [{node_id}]: Condition not met, downstream nodes will be skipped"
            )
        else:
            logger.info(
                f"->  Path Node [{node_id}]: Condition met, executing downstream nodes"
            )

        return {"condition_met": condition_met, "condition": condition}

    def _skip_downstream_nodes(self):
        """
        Skip all downstream nodes if condition not met.

        Marks all nodes downstream of this path node as skipped.
        """
        downstream_ids = self._find_downstream_ids(self.node_id, self.edges)
        for node_id in downstream_ids:
            self.coordinator.mark_node_skipped(
                node_id,
                reason="path_condition_not_met",
                details={
                    "path_node_id": self.node_id,
                    "message": "Path condition was false, skipping branch",
                },
            )
            logger.info(f"Skipping downstream node: {node_id}")

    def _find_downstream_ids(self, start_node: str, edges: list[Dict[str, Any]]) -> set:
        """
        Find all nodes downstream of start node (BFS).

        Args:
            start_node: Starting node ID
            edges: Workflow edges

        Returns:
            Set of downstream node IDs
        """
        downstream = set()
        queue = [start_node]
        visited = {start_node}

        while queue:
            current = queue.pop(0)

            for edge in edges:
                source = edge.get("source", "")
                target = edge.get("target", "")
                if source == current and target not in visited:

                    # skip fork edges (don't cross paths)
                    if edge.get("type") == "fork-branch":
                        continue
                    downstream.add(target)
                    visited.add(target)
                    queue.append(target)
        return downstream

    def _post_execution(self, result: Dict[str, Any]) -> Any:
        """
        Execute or skip downstream nodes based on condition.

        If condition is TRUE: execute downstream in parallel
        If condition is FALSE: skip all downstream

        Args:
            result: Execution result containing 'condition_met'
        """
        condition_met = result.get("condition_met", False)

        if condition_met:
            logger.info(f"Path {self.node_id} condition TRUE - executing downstream ")
            self._execute_downstream()

    def _get_downstream_edges(
        self, downstream_node_ids: set, edges: list[Dict[str, Any]]
    ) -> list[Dict[str, Any]]:
        """
        Get edges that connect downstream nodes.

        Args:
            downstream_ids: Set of downstream node IDs
            all_edges: All workflow edges

        Returns:
            Edges between downstream nodes
        """
        downstream_edges = []
        for edge in edges:
            source = edge.get("source", "")
            target = edge.get("target", "")
            # Only include if BOTH source and target are in downstream
            # This prevents KeyError in compute_dependency_levels
            if source in downstream_node_ids and target in downstream_node_ids:
                downstream_edges.append(edge)
        return downstream_edges

    def _execute_downstream(self):
        """
        Execute downstream nodes in parallel using dependency levels.

        1. Find all downstream nodes
        2. Build dependency graph
        3. Compute levels (nodes with no dependencies first)
        4. Execute each level in parallel
        5. Wait for level to complete before next level
        """

        from workflow.algorithms.topological_sort import (
            compute_dependency_levels,
            group_nodes_by_level,
        )

        downstream_node_ids = self._find_downstream_ids(self.node_id, self.edges)

        if not downstream_node_ids:
            logger.info(f"No downstream nodes to execute for path {self.node_id}")
            return

        # Get node definitions for downstream nodes
        nodes_dict = {n["id"]: n for n in self.nodes}
        downstream_nodes = [
            nodes_dict[nid] for nid in downstream_node_ids if nid in nodes_dict
        ]

        # filter edges to only downstream
        downstream_edges = self._get_downstream_edges(downstream_node_ids, self.edges)
        logger.info(
            f"Path {self.node_id} executing {len(downstream_nodes)} downstream nodes in parallel"
        )

        try:
            levels = compute_dependency_levels(downstream_nodes, downstream_edges)
            grouped_levels = group_nodes_by_level(downstream_nodes, levels)

        except Exception as e:
            logger.error(f"Failed to compute dependency levels: {e}")
            # fall back to single level
            for node in downstream_nodes:
                self._execute_single_node(node)
            return

        # Execute each level in parallel
        for level_num, nodes_at_level in enumerate(grouped_levels):
            logger.info(
                f"Path {self.node_id} executing level {level_num} with {len(nodes_at_level)} nodes in parallel"
            )
            self._execute_level(nodes_at_level)

    # execute all nodes at a given dependency level in parallel.
    def _execute_level(
        self,
        nodes_at_level: list[Dict[str, Any]],
    ):
        """
        Execute all nodes at a given dependency level in parallel.

        Args:
            nodes_at_level: List of node definitions at this level
        """
        if len(nodes_at_level) == 1:
            # Single node - execute directly
            self._execute_single_node(nodes_at_level[0])
            return

        # Get timeout from path node config (templates already resolved in _prepare_inputs)
        # This controls how long the entire level can take
        level_timeout = self.node_config.get(
            "level_timeout", 43200
        )  # 12 hours default for long-running operations

        max_workers = min(len(nodes_at_level), 10)  # limit max threads
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_node = {
                executor.submit(self._execute_single_node, node): node
                for node in nodes_at_level
            }
            try:
                for future in as_completed(future_to_node, timeout=level_timeout):
                    node = future_to_node[future]
                    node_id = node.get("id", "unknown")
                    try:
                        future.result()
                        logger.info(
                            f"[OK]  Completed downstream parallel node [{node_id}]"
                        )
                    except Exception as e:
                        logger.error(
                            f"[FAIL] Error executing node in downstream parallel [{node_id}]: {e}"
                        )
                        raise e
            except TimeoutError:
                logger.error(
                    f"[FAIL] Path level execution timeout ({level_timeout}s) exceeded"
                )
                # Cancel remaining futures
                for future in future_to_node:
                    future.cancel()
                raise TimeoutError(
                    f"Path level execution timeout ({level_timeout}s) exceeded"
                )
            except Exception as e:
                logger.error(
                    f"[FAIL] Error in executing level of downstream nodes: {e}"
                )
                for future in future_to_node:
                    future.cancel()
                raise

    def _execute_single_node(self, node: Dict[str, Any]):
        """
        Execute a single downstream node.

        Args:
            node: Node definition
        """
        node_id = node["id"]

        try:
            # check if node should be skipped (e.g., condition nodes)
            is_skipped, skip_info = self.coordinator.is_node_skipped(node_id)

            if is_skipped:
                logger.info(
                    f"[SKIP] Node {node_id} is marked as skipped, not executing. Reason: {skip_info}"
                )
                return
            logger.info(f"Executing downstream node: {node_id}")
            node_executor = create_executor(node, self.coordinator)
            node_executor.run()

        except Exception as e:
            logger.error(f"[FAIL] Error executing downstream node {node_id}: {e}")
            # Check if node has continue_on_error, otherwise re-raise
            node_config = node.get("config", {})
            if not node_config.get("error_handling", {}).get(
                "continue_on_error", False
            ):
                raise  # Re-raise to fail the path execution
