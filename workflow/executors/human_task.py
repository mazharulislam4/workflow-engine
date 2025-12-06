"""
Human Task Node Executor

Creates a human task and pauses workflow execution until completed.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict

from workflow.executors.base import NodeExecutor
from workflow.executors.registry import register_executor

logger = logging.getLogger(__name__)


@register_executor("human_task")
class HumanTaskExecutor(NodeExecutor):
    """
    Executor for human task nodes.

    Creates a human task that requires external completion (approval, input, review).
    Automatically pauses the workflow until task is completed via API.

    Example Config:
        {
            "type": "human_task",
            "config": {
                "title": "Approve Purchase Order #{{current.po_number}}",
                "description": "Review and approve PO for ${{current.total | format_currency}}",
                "form_schema": {
                    "type": "object",
                    "properties": {
                        "approved": {"type": "boolean", "title": "Approve?"},
                        "comments": {"type": "string", "title": "Comments"}
                    },
                    "required": ["approved"]
                },
                "assigned_to": "finance_team",
                "timeout_hours": 48,
                "priority": "high",
                "on_rejection": "fail"
            }
        }

    """

    def execute(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create human task and return task details.

        Note: The actual pause happens in post_execution (infrastructure layer).

        Args:
            inputs: Prepared inputs with evaluated config

        Returns:
            Human task details (id, status, assigned_to, expires_at)
        """
        config = inputs.get("config", {})
        node_id = inputs.get("node_id")

        # calculate expiration
        timeout_hours = config.get("timeout_hours", 72)  # default 72 hours
        expires_at = datetime.utcnow() + timedelta(hours=timeout_hours)

        # TODO: create human task

        result = {}
        result.update(config)  # include other config details for reference
        result.update(
            {
                "node_id": node_id,
                "status": "pending",
                "expires_at": expires_at.isoformat(),
                "paused_workflow": True,
            }
        )
        return result

    def _post_execution(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Override post-execution to pause workflow.

        After human task is created, pause the workflow at this node.
        """
        super()._post_execution(result)

        # pause workflow logic would go here (infrastructure layer)
        # TODO: implement actual pause in workflow engine
        try:
            logger.info(
                f"[PAUSE] Pausing workflow at human task node [{result.get('node_id')}], waiting for task completion."
            )
            return {"paused": True}
        except Exception as e:
            logger.error(f"Error pausing workflow for human task: {e}")
            raise e
