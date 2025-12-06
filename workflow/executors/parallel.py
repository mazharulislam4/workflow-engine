"""
Parallel Node Executor

Acts as a synchronization marker for concurrent execution.
All downstream nodes execute in parallel (background).

Configuration:
- wait_for_completion: true → Wait for all parallel tasks
- wait_for_completion: false → Continue immediately
- max_concurrent: Max parallel tasks (default: 20)
"""

import json
import logging
from ast import Set
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed
from typing import Any, Dict, List, Set

from workflow.executors.base import NodeExecutor
from workflow.executors.registry import create_executor, register_executor

logger = logging.getLogger(__name__)


@register_executor("parallel")
class ParallelNodeExecutor(NodeExecutor):
    """
    Parallel node - synchronization marker for concurrent execution.

    The parallel node itself does nothing except:
    1. Find all downstream nodes
    2. Execute them concurrently (always in background)
    3. Optionally wait for completion before marking parallel node as done

    JSON Structure:
    {
        "id": "parallel1",
        "type": "parallel",
        "config": {
            "wait_for_completion": true,  // Wait for all tasks?
            "timeout": 300                 // Timeout in seconds (null = infinite)
        }
    }

    Workflow edges define what runs in parallel:
    {
        "edges": [
            {"source": "node1", "target": "parallel1"},
            {"source": "parallel1", "target": "task_a"},  // Runs in parallel
            {"source": "parallel1", "target": "task_b"},  // Runs in parallel
            {"source": "parallel1", "target": "task_c"},  // Runs in parallel
            {"source": "parallel1", "target": "node2"}    // Next node (after wait)
        ]
    }
    """

    def execute(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute parallel marker node.

        1. Find all downstream nodes
        2. Execute them concurrently
        3. Wait for completion (if configured)

        Args:
            inputs: Contains 'config'

        Returns:
            Dict with execution status
        """
        config = inputs.get("config", {})
        node_id = inputs.get("node_id", "unknown")

        wait_for_completion = config.get("wait_for_completion", True)
        parallel_timeout = config.get("timeout", 43200)  # default: 12 hours
        max_concurrent = 20  # always use 20 as default max concurrent

        # Find all downstream nodes
        downstream_ids = self._find_downstream_nodes(self.node_id, self.edges)

        if not downstream_ids:
            logger.warning(f"Parallel node [{node_id}] has no downstream nodes.")
            return {"status": "completed", "tasks": 0}

        if len(downstream_ids) > max_concurrent:
            raise ValueError(
                f"Parallel node [{node_id}] has {len(downstream_ids)} downstream nodes, "
                f"which exceeds the max concurrent limit of {max_concurrent}."
            )

        node_dict = {n["id"]: n for n in self.nodes if n["id"] in downstream_ids}

        if wait_for_completion:
            results = self._execute_parallel_wait(
                nodes=[node_dict[nid] for nid in downstream_ids],
                timeout=parallel_timeout,
            )
            logger.info(
                f"Parallel node [{node_id}] completed all tasks: {json.dumps(results)}"
            )
            return {
                "status": "completed",
                "tasks": len(downstream_ids),
                "results": results,
            }

        else:
            self._execute_parallel_background(
                nodes=[node_dict[nid] for nid in downstream_ids]
            )
            logger.info(f"Parallel node [{node_id}] started all tasks in background.")
            return {
                "status": "started_in_background",
                "tasks": len(downstream_ids),
            }

    def _find_downstream_nodes(
        self, node_id: str, edges: List[Dict[str, Any]]
    ) -> Set[str]:
        """Find all nodes directly connected from this parallel node."""
        downstream = set()
        for edge in edges:
            if edge.get("source") == node_id:
                downstream.add(edge.get("target"))

        return downstream

    def _execute_single_node(self, node: Dict[str, Any]) -> Any:
        """Execute a single node."""
        node_id = node.get("id", "unknown")

        # if skipped
        is_skipped, skip_reason = self.coordinator.is_node_skipped(node_id)
        if is_skipped:
            logger.info(f"[SKIP] Node [{node_id}] is skipped. Reason: {skip_reason}")
            return {"status": "skipped", "reason": skip_reason}

        # create and execute node
        executor = create_executor(node, self.coordinator)
        executor.run()

    def _execute_single_safe(self, node: Dict[str, Any]) -> None:
        """Execute node with error handling for background execution."""
        node_id = node.get("id", "unknown")
        try:
            logger.info(f"[Background] Executing parallel task {node_id}")
            self._execute_single_node(node)
            logger.info(f"[Background] Parallel task {node_id} completed")
        except Exception as e:
            logger.error(
                f"[Background] Parallel task {node_id} failed: {e}", exc_info=True
            )

    def _execute_parallel_background(self, nodes: List[Dict]):
        """
        Execute nodes in background (fire-and-forget).
        Spawns daemon threads that don't block.

        Args:
            nodes: Nodes to execute
        """
        import threading

        logger.info(f"Starting {len(nodes)} parallel tasks in background")

        for node in nodes:
            node_id = node.get("id", "unknown")
            thread = threading.Thread(
                target=self._execute_single_safe,
                args=(node,),
                name=f"parallel-{node_id}",
                daemon=True,
            )
            thread.start()
            logger.info(f"Started background task {node_id}")

        logger.info(f"All {len(nodes)} parallel tasks started - continuing workflow")

    def _execute_parallel_wait(self, nodes: List[Dict], timeout: int) -> Dict:
        """
        Execute nodes in parallel and WAIT for all to complete.

        Args:
            nodes: Nodes to execute
            timeout: Timeout in seconds (None = infinite)

        Returns:
            Dict of results
        """
        results = {}
        max_workers = min(len(nodes), 20)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._execute_single_node, node): node["id"]
                for node in nodes
            }

            try:
                for future in as_completed(futures, timeout=timeout):
                    node_id = futures[future]
                    try:
                        future.result()
                        results[node_id] = "completed"
                        logger.info(f"Parallel task {node_id}   completed successfully")
                    except Exception as e:
                        results[node_id] = f"failed: {e}"
                        logger.error(f"Parallel task {node_id} failed: {e}")
            except TimeoutError:
                timeout_val = "infinite" if timeout is None else f"{timeout}s"
                logger.error(f"Parallel group timed out after {timeout_val}")
                # Cancel remaining
                for future, node_id in futures.items():
                    if node_id not in results:
                        future.cancel()
                        results[node_id] = "cancelled"

        return results
