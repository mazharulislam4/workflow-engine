"""
Loop Node Executor

Iterates over arrays and executes child nodes for each item.
Supports loop variables: {{loop.item}}, {{loop.index}}, etc.

Uses pure function pattern with coordinator for child execution.
"""

import logging
from typing import Any, Dict

from workflow.executors.base import NodeExecutor
from workflow.executors.registry import create_executor, register_executor

logger = logging.getLogger(__name__)


@register_executor("loop")
class LoopNodeExecutor(NodeExecutor):
    """
    Executor for Loop nodes.

    Iterates over an array and executes nested nodes for each item.
    Provides loop context variables:
    - {{loop.item}} or {{loop.<alias>}} - current item
    - {{loop.index}} - current index (0-based)
    - {{loop.count}} - total number of items
    - {{loop.is_first}} - true if first iteration
    - {{loop.is_last}} - true if last iteration

    Pure function pattern: Uses coordinator for child node execution.
    """

    def execute(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        loop execution.

        Args:
            inputs: Contains 'config'

        Returns:
            Dict with iteration results
        """

        config = inputs.get("config", {})
        node_id = inputs.get("node_id", "unknown")

        items = config.get("items", [])

        if not items:
            raise ValueError("Loop 'items' must be provided.")
        if not isinstance(items, list):
            raise ValueError("Loop 'items' must be a list/array.")

        # Get loop results from coordinator
        alias = config.get("item_alias", "item")
        childe_nodes = config.get("nodes", [])

        loop_results = []

        for index, item in enumerate(items):
            loop_context = {
                "item": item,
                "alias": alias,
                "index": index,
                "len": len(items),
                "is_first": index == 0,
                "is_last": index == len(items) - 1,
            }
            self.context.set_loop("loop", loop_context)
            logger.info(f"Loop iteration [{node_id}]: index={index}, item={item}")
            iteration_result = self._execute_iteration(
                child_nodes=childe_nodes, loop_context=loop_context
            )
            loop_results.append(iteration_result)

        # clear loop context
        self.context.clear_loop()
        return {"results": loop_results, "total_iterations": len(loop_results)}

    def _execute_iteration(
        self, child_nodes: list, loop_context: dict
    ) -> Dict[str, Any]:
        """
        Execute a single loop iteration.

        Args:
            child_nodes: List of child node configs to execute
            loop_context: Current loop context variables

        Returns:
            Dict with iteration results
        """
        iteration_outputs = {}

        for child_node in child_nodes:
            # create executor for child node
            executor = create_executor(node=child_node, coordinator=self.coordinator)

            executor.run()

            # get results
            child_node_id = child_node.get("id")
            results = self.coordinator.get_node_output(child_node_id)
            if results is not None:
                iteration_outputs[child_node_id] = results

        return {
            "index": loop_context["index"],
            "outputs": iteration_outputs,
            "item": loop_context["item"],
        }
