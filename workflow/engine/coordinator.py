"""
Execution Coordinator

Mediator between WorkflowExecutor and NodeExecutors.
Decouples orchestration from execution through a clean interface.

Pattern: Mediator + Observer

"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from workflow.engine.context import ContextManager

logger = logging.getLogger(__name__)


class ExecutionCoordinator:
    """
    Mediator for workflow execution.

    Provides communication interface between WorkflowExecutor (orchestrator)
    and NodeExecutors (executors).

    Responsibilities:
    - Track skip state for nodes
    - Provide shared execution context
    - Record execution events
    - Enable node-to-node communication
    """

    def __init__(self, run_id: str, input_data: Optional[Dict] = None):
        """
        Initialize coordinator.

        Args:
            run_id: Workflow run ID
            input_data: Input data for workflow
        """

        self.run_id = run_id
        self.input_data = input_data or {}
        self.skip_nodes = set()
        self.context: ContextManager = ContextManager()
        # skip tracking
        self._skipped_nodes: Dict[str, Dict[str, Any]] = {}

        # execution state tracking
        self._execution_events: List[Dict[str, Any]] = []
        self._node_states: Dict[str, str] = {}

        # Workflow-Level flags
        self._should_halt: bool = False
        self._halt_reason: Optional[str] = None

    def mark_node_skipped(
        self, node_id: str, reason: str, details: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Mark a node as skipped.
        called by NodeExecutors to indicate skip status.
        E.g., ForkNodeExecutor marks non-selected branch nodes as skipped.
        Args:
            node_id: ID of node to skip
            reason: Why it should be skipped
            details: Additional context (optional)
        """
        self._skipped_nodes[node_id] = {
            "reason": reason,
            "details": details or {},
        }

        logger.debug(f"Node {node_id} marked as skipped: {reason}")

    def is_node_skipped(self, node_id: str) -> tuple[bool, Optional[Dict[str, Any]]]:
        """
        Check if a node is marked as skipped.

        Args:
            node_id: ID of node to check
        Returns:
            True if node is skipped, False otherwise , along with skip reason/details if skipped
        """
        if node_id in self._skipped_nodes:
            return True, self._skipped_nodes[node_id]
        return False, None

    def get_all_skipped_nodes(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all skipped nodes with reasons.

        Returns:
            Dict of node_id to skip reason/details
        """
        return self._skipped_nodes

    def get_context(self) -> ContextManager:
        """
        Get shared execution context.

        Returns:
            ContextManager instance
        """
        return self.context

    def set_node_output(self, node_id: str, output_data: Dict[str, Any]) -> None:
        """
        Set output data for a specific node in the context.

        Args:
            node_id: ID of the node
            output_data: Output data to set
        """

        self.context.update_step(node_id, outputs=output_data)

    def get_node_output(self, node_id: str) -> Dict[str, Any]:
        """
        Get output data for a specific node from the context.

        Args:
            node_id: ID of the node
        Returns:
            Output data of the node
        """
        step_data = self.context.get_step(node_id)
        return step_data.get("outputs", {})

    def set_node_input(self, node_id: str, input_data: Dict[str, Any]) -> None:
        """
        Set input data for a specific node in the context.

        Args:
            node_id: ID of the node
            input_data: Input data to set
        """

        self.context.update_step(node_id, inputs=input_data)

    def get_node_input(self, node_id: str) -> Dict[str, Any]:
        """
        Get input data for a specific node from the context.

        Args:
            node_id: ID of the node
        Returns:
            Input data of the node
        """
        step_data = self.context.get_step(node_id)
        return step_data.get("inputs", {})

    # stage management methods
    def set_node_state(self, node_id: str, state: str) -> None:
        """
        Set execution state for a specific node.

        Args:
            node_id: ID of the node
            state: Execution state (e.g., "pending", "running", "completed", "failed")
        """
        self._node_states[node_id] = state

    # event recording methods
    def record_event(
        self,
        event_type: str,
        node_id: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Record an execution event.

        Args:
            event_type: Type of the event
            node_id: Optional ID of the node related to the event
            data: Optional additional data for the event
        """
        event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": event_type,
            "node_id": node_id,
            "data": data or {},
        }
        self._execution_events.append(event)
        logger.debug(f"Recorded event: {event}")

    def get_events(self, event_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get all recorded execution events.

        Returns:
            List of execution events
        """
        if event_type:
            return [e for e in self._execution_events if e["event_type"] == event_type]
        return self._execution_events.copy()

    def get_execution_events(self) -> List[Dict[str, Any]]:
        """
        Get all execution events (alias for get_events).

        Returns:
            List of execution events
        """
        return self.get_events()

    def get_node_state(self, node_id: str) -> Optional[str]:
        """
        Get the current state of a node.

        Args:
            node_id: ID of the node

        Returns:
            Node state or None if not found
        """
        return self._node_states.get(node_id)

    # workflow-level control methods
    def halt_workflow(self, reason: str) -> None:
        """
        Signal to halt the entire workflow execution.

        Args:
            reason: Reason for halting the workflow
        """
        self._should_halt = True
        self._halt_reason = reason
        logger.warning(f"Workflow halted: {reason}")
        self.record_event("workflow_halt_requested", data={"reason": reason})

    def should_halt(self) -> tuple[bool, Optional[str]]:
        """
        Check if the workflow execution should be halted.

        Returns:
            Tuple of (should_halt: bool, reason: Optional[str])
        """
        return self._should_halt, self._halt_reason

    # utility methods
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get execution statistics.

        Returns:
            Dict of execution statistics
        """
        return {
            "total_events": len(self._execution_events),
            "skipped_nodes": len(self._skipped_nodes),
            "node_states": self._node_states.copy(),
            "should_halt": self._should_halt,
        }

    def __repr__(self) -> str:
        return f"ExecutionCoordinator(run_id={self.run_id}, events={len(self._execution_events)})"
