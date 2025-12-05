"""
Start Node Executor

Entry point for workflow execution.
"""

import json
import logging
from typing import Any, Dict

from workflow.executors.base import NodeExecutor
from workflow.executors.registry import register_executor

logger = logging.getLogger(__name__)


@register_executor("start")
class StartNodeExecutor(NodeExecutor):
    """
    Executor for Start nodes.

    Start nodes mark the entry point of a workflow.
    They don't perform any actual work, just signal the beginning.

    Pure function pattern: No dependencies, just returns empty dict.
    """

    def execute(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Start node execution.

        Args:
            inputs: Not used for start node

        Returns:
            Empty dictionary (no output)
        """
        node_id = inputs.get("node_id", "unknown")
        logger.info(f"🚀 Starting workflow execution at node: {node_id}")
        logger.debug(f"Start node inputs: {json.dumps(inputs, indent=2, default=str)}")
        return {}
