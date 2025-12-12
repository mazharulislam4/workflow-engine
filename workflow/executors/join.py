"""
Join Node Executor

Aggregates results from fork or parallel nodes.
Acts as a synchronization point: fork/parallel → join → regular flow

Configuration:
- source: ID of fork/parallel node to join
- strategy: How to aggregate results (merge, list, first, custom)
"""

import logging
from typing import Any, Dict

from workflow.executors.base import NodeExecutor
from workflow.executors.registry import register_executor

logger = logging.getLogger(__name__)


@register_executor("join")
class JoinNodeExecutor(NodeExecutor):
    """
    Join node - aggregates results from fork/parallel execution.

    The join node:
    1. Waits for source node (fork/parallel) to complete
    2. Collects all results from the source
    3. Aggregates them based on strategy
    4. Makes aggregated result available to downstream nodes

    JSON Structure:
    {
        "id": "join1",
        "type": "join",
        "config": {
            "source": "fork1",           // ID of fork/parallel to join
            "strategy": "merge",         // merge | list | first | count | outputs
            "filter": null,               // Optional: "success" | "failed"
            "timeout": 60                 // Wait timeout (null = infinite)
        }
    }

    Workflow pattern:
    {
        "edges": [
            {"source": "fork1", "target": "path_a", "type": "fork-branch"},
            {"source": "fork1", "target": "path_b", "type": "fork-branch"},
            {"source": "fork1", "target": "join1"},   // Join after fork
            {"source": "join1", "target": "next_node"}
        ]
    }

    Available in downstream nodes:
    {{steps.join1.aggregated}}      - Combined results
    {{steps.join1.count}}           - Number of results
    {{steps.join1.source}}          - Source node ID
    """

    def execute(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute join operation.

        1. Get source node results
        2. Aggregate based on strategy
        3. Return aggregated result

        Args:
            inputs: Contains 'config'

        Returns:
            Dict with aggregated results
        """
        config = inputs.get("config", {})
        node_id = inputs.get("node_id", "unknown")
        source_id = config.get("source")
        strategy = config.get("strategy", "merge")
        filter_expr = config.get("filter", None)
        timeout = config.get("timeout", None)  # in seconds

        if not source_id:
            return {}

        logger.info(f"Join node [{node_id}] joining from source [{source_id}]")

        # get source output from inputs or context
        source_output = self.context.get_step(source_id).get("outputs", {})

        if not source_output:
            logger.warning(
                f"Join node [{node_id}] found no output from source [{source_id}]"
            )
            return {
                "source": source_id,
                "strategy": strategy,
                "aggregated": None,
                "count": 0,
                "status": "no_data",
            }

        # check if source is fork or parallel
        source_type = source_output.get("type", "unknown")

        if source_type == "fork":
            results = self._extract_fork_results(source_output)
        elif source_type == "parallel":
            results = self._extract_parallel_results(source_output)
        else:
            results = [source_output]

        logger.info(
            f"Join {self.node_id}: collected {len(results)} results from {source_id}"
        )

        # Apply filter if specified
        if filter_expr:
            results = self._filter_results(results, filter_expr)
            logger.info(f"Join {self.node_id}: {len(results)} results after filter")

        # Aggregate based on strategy
        aggregated = self._aggregate_results(results, strategy)

        return {
            "source": source_id,
            "strategy": strategy,
            "aggregated": aggregated,
            "count": len(results),
            "status": "completed",
        }

    def _extract_fork_results(
        self, source_output: Dict[str, Any]
    ) -> list[Dict[str, Any]]:
        """
        Extract results from fork node output.

        Fork output structure:
        {
            "type": "fork",
            "total_paths": 2,
            "paths_executed": 2,
            "paths": {
                "path_a": {
                    "condition_met": true,
                    "nodes": {
                        "task_a1": {"status": "success", "output": {...}},
                        "task_a2": {"status": "success", "output": {...}}
                    },
                    "status": "success"
                },
                "path_b": {...}
            }
        }
        """
        results = []
        paths = source_output.get("paths", {})
        for path_id, path_data in paths.items():
            # Only include paths that executed (condition_met = true)
            if path_data.get("condition_met", False):
                # Collect all node outputs from this path
                nodes = path_data.get("nodes", {})
                path_result = {
                    "path_id": path_id,
                    "status": path_data.get("status", "unknown"),
                    "nodes": nodes,
                }
                results.append(path_result)
        return results

    def _extract_parallel_results(
        self, source_output: Dict[str, Any]
    ) -> list[Dict[str, Any]]:
        """Extract results from parallel node output."""
        results = []
        tasks = source_output.get("results", {})
        for task_id, task_data in tasks.items():
            if isinstance(task_data, dict):
                results.append(task_data)
        return results

    def _filter_results(
        self, results: list[Dict[str, Any]], filter_expr: str
    ) -> list[Dict[str, Any]]:
        """Filter results based on filter expression."""
        filtered = []
        for result in results:
            status = result.get("status", "unknown").lower()
            if filter_expr == "success" and status in ["success", "completed"]:
                filtered.append(result)
            elif filter_expr == "failed" and status not in ["success", "completed"]:
                filtered.append(result)
        return filtered

    def _aggregate_results(self, results: list[Dict[str, Any]], strategy: str) -> Any:
        """
        Aggregate results based on strategy.

        Strategies:
        - merge: Merge all node outputs into a single dict
        - list: Return list of all path results
        - first: Return first path result
        - count: Return count of paths
        - outputs: Extract just the node outputs from each path
        """
        if strategy == "merge":
            # Merge all node outputs from all paths into one dict
            aggregated = {}
            for result in results:
                nodes = result.get("nodes", {})
                for node_id, node_data in nodes.items():
                    output = node_data.get("output", {})
                    if output:
                        aggregated[node_id] = output
            return aggregated
        elif strategy == "list":
            return results
        elif strategy == "first":
            return results[0] if results else None
        elif strategy == "count":
            return len(results)
        elif strategy == "outputs":
            # Extract node outputs from each path
            outputs = []
            for result in results:
                nodes = result.get("nodes", {})
                for node_id, node_data in nodes.items():
                    output = node_data.get("output", {})
                    if output:
                        outputs.append({node_id: output})
            return outputs
        else:
            logger.warning(f"Join node: unknown aggregation strategy '{strategy}'")
            return None
