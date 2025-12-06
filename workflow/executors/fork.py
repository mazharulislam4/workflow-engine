"""
Fork Node Executor with Celery and Enhanced Limits

Uses Celery for distributed execution instead of threads.
Supports both per-path and overall fork limits.
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from concurrent.futures import as_completed
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
        node_id = inputs.get("node_id", "unknown")
        wait_for_completion = config.get("wait_for_completion", True)
        max_workers = config.get("max_workers", 10)
        max_nodes_per_path = config.get("max_nodes_per_path", 50)
        max_total_nodes = config.get("max_total_nodes", 200)
        execution_mode = config.get("execution_mode", "thread")

        logger.info(
            f"[FORK] Fork Node [{node_id}]: Starting execution in {execution_mode} mode"
        )
        logger.debug(f"Fork Config:\n{json.dumps(config, indent=2, default=str)}")
        logger.debug(
            f"Max Workers: {max_workers}, Max Nodes Per Path: {max_nodes_per_path}, Max Total: {max_total_nodes}"
        )

        # find all path nodes
        path_nodes = self._get_path_nodes()

        if not path_nodes:
            logger.warning(f"[WARN]  Fork Node [{self.node_id}]: No path nodes found")
            return self._create_fork_output({})

        path_ids = [p.get("id") for p in path_nodes]
        logger.info(
            f"[FORK] Fork Node [{self.node_id}]: Found {len(path_nodes)} paths: {path_ids}"
        )

        # validate limits
        self._validate_limits(path_nodes, max_nodes_per_path, max_total_nodes)
        path_results = {}
        # execute paths
        if execution_mode == "celery":
            path_results = {}
            # TODO: Implement Celery-based execution later
        else:
            if wait_for_completion:
                path_results = self._execute_path_thread(
                    path_nodes, max_workers, config
                )
            else:
                self._execute_path_thread_background(path_nodes, max_workers)

        paths_executed = len(
            [p for p in path_results.values() if p.get("condition_met")]
        )
        logger.info(
            f"[OK] Fork Node [{self.node_id}]: Completed - {paths_executed}/{len(path_results)} paths executed"
        )
        logger.debug(
            f"Fork Results Summary:\n{json.dumps({k: {**v, 'nodes': f"{len(v.get('nodes', {}))} nodes"} for k, v in path_results.items()}, indent=2, default=str)}"
        )

        return self._create_fork_output(path_results)

    def _create_fork_output(self, path_results: Dict) -> Dict:
        """
        Create nested fork output structure.

        Args:
            path_results: Dict of {path_id: path_result}

        Returns:
            Nested structure for context storage
        """
        return {
            "type": "fork",
            "total_paths": len(path_results),
            "paths_executed": len(
                [p for p in path_results.values() if p.get("condition_met")]
            ),
            "paths": path_results,  # Nested: path_id -> nodes -> node_id -> output
        }

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

    def _collect_path_nodes(self, path_id: str) -> Dict[str, Any]:
        """
        Collect all node outputs from this path.

        Args:
            path_id: Path ID

        Returns:
            Dict of {node_id: node_output}
        """
        downstream_ids = self._find_downstream_ids(path_id, self.edges)

        # Get outputs from context
        nodes = {}
        for node_id in downstream_ids:
            output = self.coordinator.get_node_output(node_id)
            if output is not None:
                nodes[node_id] = {"status": "success", "output": output}
        return nodes

    def _find_downstream_ids(self, path_id: str, edges: List[Dict[str, Any]]) -> set:
        """Find all downstream node IDs (BFS)"""

        downstream = set()
        queue = [path_id]
        visited = {path_id}

        while queue:
            current = queue.pop(0)
            for edge in edges:
                source = edge.get("source", "")
                target = edge.get("target", "")
                if source == current and target not in visited:
                    downstream.add(target)
                    visited.add(target)
                    queue.append(target)

        return downstream

    def _execute_single_path(self, path_node: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a single path node.

        Args:
            path_node: The path node to execute

        Returns:
            Result of path execution
        """
        from workflow.executors.registry import create_executor

        path_id = path_node.get("id", "")

        try:
            logger.info(f"Executing path [{path_id}]")
            path_executor = create_executor(path_node, self.coordinator)

            # execute path (evaluates condition and executes downstream nodes)
            path_executor.run()

            # collet all nodes that executed in this path
            downstream_nodes = self._collect_path_nodes(path_id)

            return {
                "condition_met": True,
                "nodes": downstream_nodes,
                "status": "success",
            }

        except Exception as e:
            logger.error(f"[FAIL] Error executing path [{path_id}]: {e}")
            return {
                "condition_met": False,
                "nodes": {},
                "status": "error",
                "error": str(e),
            }

    def _execute_path_thread(
        self, path_nodes: List[Dict[str, Any]], max_workers: int, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute paths using threads with timeout support.

        Args:
            path_nodes: List of path nodes to execute
            max_workers: Maximum number of parallel workers
            config: Evaluated configuration dictionary with templates resolved

        Returns:
            Nested dict with path results
        """
        if len(path_nodes) == 1:
            # Single path - execute directly
            path_id = path_nodes[0].get("id", "")
            result = self._execute_single_path(path_nodes[0])
            return {path_id: result}

        # Get timeout from evaluated fork config (templates already resolved)
        fork_timeout = config.get(
            "timeout", 43200
        )  # 12 hours default for parallel long-running operations

        results = {}
        with ThreadPoolExecutor(max_workers=max_workers or 10) as executor:
            future_to_path = {
                executor.submit(self._execute_single_path, path_node): path_node
                for path_node in path_nodes
            }

            try:
                for future in as_completed(future_to_path, timeout=fork_timeout):
                    path_node = future_to_path[future]
                    path_id = path_node.get("id", "unknown")
                    try:
                        path_result = future.result()
                        results[path_id] = path_result
                        logger.info(f"Completed path [{path_id}]")
                    except Exception as e:
                        logger.error(f"[FAIL] Error executing path [{path_id}]: {e}")
                        results[path_id] = {
                            "condition_met": False,
                            "status": "error",
                            "error": str(e),
                            "nodes": {},
                        }
                # All paths completed - continue to return results
            except FuturesTimeoutError:
                logger.error(
                    f"[FAIL] Fork execution timeout ({fork_timeout}s) exceeded"
                )
                # Cancel remaining futures
                for future in future_to_path:
                    future.cancel()
                raise TimeoutError(f"Fork execution timeout ({fork_timeout}s) exceeded")

        return results

    def _execute_path_thread_background(
        self,
        path_nodes: List[Dict[str, Any]],
        max_workers: int,
    ) -> None:
        """
        Execute paths via THREADS in background (fire-and-forget).
        Starts daemon threads that don't block workflow execution.
        """

        import threading

        logger.info(f"Starting {len(path_nodes)} paths in background (daemon threads)")

        for path_node in path_nodes:
            path_id = path_node.get("id", "unknown")
            thread = threading.Thread(
                target=self._execute_single_path_safe, args=(path_node,), daemon=True
            )
            thread.start()
            logger.info(f"-> Started background execution for path [{path_id}]")

        logger.info(
            f"All {len(path_nodes)} background paths started - continuing workflow"
        )

    def _execution_paths_background(
        self, path_nodes: List[Dict[str, Any]], max_workers: int, execution_mode: str
    ):
        """
        Execute paths in background (fire-and-forget).
        Starts daemon threads that don't block workflow execution.
        """
        import threading

        logger.info(
            f"[FORK] Fork Node [{self.node_id}]: Starting background execution in {execution_mode} mode"
        )

        for path_node in path_nodes:
            path_id = path_node.get("id", "unknown")
            thread = threading.Thread(
                target=self._execute_single_path_safe, args=(path_node,), daemon=True
            )
            thread.start()
            logger.info(f"-> Started background execution for path [{path_id}]")

        logger.info(
            f"[OK] Fork Node [{self.node_id}]: Background execution started for {len(path_nodes)} paths"
        )

    def _execute_single_path_safe(self, path_node: Dict[str, Any]) -> None:
        """
        Wrapper to execute a single path with error handling.

        Args:
            path_node: The path node to execute

        Returns:
            Result of path execution
        """
        path_id = path_node.get("id", "unknown")
        try:
            logger.info(f"[Background] Executing path {path_id}")
            self._execute_single_path(path_node)
            logger.info(f"[Background] Path {path_id} completed")
        except Exception as e:
            logger.error(f"[Background] Path {path_id} failed: {e}", exc_info=True)
