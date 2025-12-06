"""
End Node Executor

Exit point for workflow execution.
"""

import json
import logging
from typing import Any, Dict

from workflow.executors.base import NodeExecutor
from workflow.executors.registry import register_executor

logger = logging.getLogger(__name__)


@register_executor("end")
class EndNodeExecutor(NodeExecutor):
    """
    Executor for End nodes.

    End nodes mark the completion of a workflow.
    They don't perform any actual work, just signal completion.

    Pure function pattern: No dependencies, just returns empty dict.
    """

    def execute(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        End node execution.

        Args:
            inputs: Not used for end node

        Returns:
            Empty dictionary (no output)
        """
        node_id = inputs.get("node_id", "unknown")
        logger.info(f"Workflow completed at node: {node_id}")
        logger.debug(f"End node inputs: {json.dumps(inputs, indent=2, default=str)}")
        return {}
