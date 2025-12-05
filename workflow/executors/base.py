"""
Base Node Executor
Abstract base class for all node executors.
Defines the interface that all node types must implement.

"""

import logging
import traceback
from abc import ABC, abstractmethod
from math import e
from re import A, S
from typing import Any, Dict, List, Optional, Tuple

from django.utils import timezone

from workflow.engine.context import ContextManager
from workflow.engine.coordinator import ExecutionCoordinator

logger = logging.getLogger(__name__)


class NodeExecutor(ABC):
    """
    Abstract base class for node executors.
    All node executors must implement the execute method.

    Template Method Pattern: run() orchestrates the lifecycle,
    subclasses implement execute() with specific logic.
    """

    def __init__(
        self,
        node: Dict[str, Any],
        coordinator: ExecutionCoordinator,
    ):
        self.node = node
        self.coordinator = coordinator
        self.context = coordinator.get_context()
        self.variables = self.context.get_variable("variables")
        self.node_id = node.get("id", "")
        self.node_type = node.get("type", "")
        self.node_config = node.get("config", {})
        self.workflow_instance = self.context.get_workflow_executor()
        self.edges = self.workflow_instance.edges or []
        self.nodes = self.workflow_instance.nodes or []

        logger.debug(f"edges in NodeExecutor: {self.edges}")

        self.run_node = None  # Placeholder for WorkflowRunNode record

    # ==================== Main Lifecycle ====================

    def run(self):
        """
        Execute full node lifecycle.

        Template Method: orchestrates the execution flow.
        Subclasses should NOT override this - implement execute() instead.

        Steps:
        1. Pre-execution checks
        2. Database record creation
        3. Execution with retries
        4. Success/error handling
        5. Post-execution tasks
        """

        try:
            if not self.node_id:
                raise ValueError("Node ID is missing.")
            # Record start event
            self.coordinator.record_event("node_started", self.node_id)
            self.coordinator.set_node_state(str(self.node_id), "running")

            # 1. Check if should skip

            should_skip, skip_reason = self._should_skip()

            if should_skip:
                self._record_skipped(skip_reason)
                self.coordinator.record_event(
                    "node_skipped", self.node_id, {"reason": skip_reason}
                )
                return

            # 2. Create WorkflowRunNode record
            self._create_run_node_record()

            # 3. Execute with retries
            result = self._execute_with_retries()
            # 4. Handle success
            self._record_success(result)

            # Save step output to context
            self.context.set_step(
                step_key=self.node_id,
                inputs=self._prepare_inputs(),
                outputs=result,
                options={},
            )

            # 5. Post-execution tasks
            self._post_execution(result=result)

            # 6. Handle success/error edge routing
            if self._has_error_routing_edges():
                self._route_to_success_edge(result)

            self.coordinator.record_event(
                "node_completed", self.node_id, {"status": "success", "result": result}
            )
            self.coordinator.set_node_state(str(self.node_id), "success")

        except Exception as er:
            # Handle error
            if self._has_error_routing_edges():
                self._handle_error_with_routing(er)
                self.coordinator.record_event(
                    "node_failed", self.node_id, {"error": str(er)}
                )
                self.coordinator.set_node_state(str(self.node_id), "failed")
            else:
                # No error routing - fail normally
                import traceback

                self._handle_error(er, traceback.format_exc())
                self.coordinator.record_event(
                    "node_failed", self.node_id, {"error": str(er)}
                )
                self.coordinator.set_node_state(str(self.node_id), "failed")

    # ==================== Skip logic ====================

    def _should_skip(self) -> Tuple[bool, Optional[str]]:
        """
        Determine if this node should skip execution.

        Override in subclasses for node-specific skip logic.
        E.g., ConditionNodeExecutor might skip based on parent condition result.

        Returns:
            Tuple of (should_skip, reason)
        """

        # Check if coordinator indicates to skip
        is_skipped, skip_info = self.coordinator.is_node_skipped(self.node_id)
        if is_skipped:
            return True, skip_info.get("reason", "marked_for_skip")
        return False, None

    # ==================== Execution with Retry ====================

    def _execute_with_retries(self) -> Any:
        """
        Execute node with retry logic.

        Returns:
            Execution result

        Raises:
            Exception: If all retries exhausted
        """

        retry_config = self.node_config.get("retry", {})
        max_retries = retry_config.get("max_retries", 0)
        retry_delay = retry_config.get("delay_seconds", 0.3)  # in seconds

        last_exception = None

        for attempt in range(max_retries + 1):
            try:
                # TODO: Update attempt count in database
                logger.debug(
                    f"Executing {self.node_id}, attempt {attempt + 1}/{max_retries + 1}"
                )

                # Execute the node
                start_time = timezone.now()
                result = self.execute_adapter()
                end_time = timezone.now()

                # Record execution time
                execution_time_ms = int((end_time - start_time).total_seconds() * 1000)

                # TODO: update execution time
                logger.debug(f"Node {self.node_id} executed in {execution_time_ms} ms")

                return result
            except Exception as e1:
                last_exception = e1
                if attempt < max_retries:
                    logger.warning(
                        f"Node {self.node_id} execution failed on attempt {attempt + 1}: {e1}"
                    )
                    self.coordinator.record_event(
                        "node_retry_failed",
                        self.node_id,
                        {
                            "attempt": attempt + 1,
                            "error": str(e1),
                        },
                    )

                    if retry_delay > 0:
                        import time

                        time.sleep(retry_delay)
                else:
                    logger.error(
                        f"Node {self.node_id} execution failed after {max_retries + 1} attempts."
                    )
                    raise
            # Shouldn't reach here, but just in case
            if last_exception:
                raise last_exception

    # ==================== Error Handling====================

    def _handle_error(self, error: Exception, error_trace: Optional[str]) -> None:
        """
        Handle execution error.

        Implements continue_on_error and success/error path routing.

        Args:
            error: Exception that occurred
        """
        error_handling = self.node_config.get("error_handling", {})
        continue_on_error = error_handling.get("continue_on_error", False)

        # TODO: Update database with error

        if continue_on_error:
            # Don't fail workflow, but handle routing
            logger.warning(
                f"Node {self.node_id} failed but continuing due to continue_on_error: {error}"
            )
            # Mark success path nodes as skipped (taking error path instead)
            success_node_id = error_handling.get("on_success")

            if success_node_id:
                self.coordinator.mark_node_skipped(
                    success_node_id,
                    reason="error_path_taken",
                    details={"source_node": self.node_id, "error": str(error)},
                )

        else:
            # Fail the workflow execution
            logger.error(f"Node {self.node_id} failed: {error}")
            # Reraise to propagate failure
            raise error

    def _has_error_routing_edges(self) -> bool:
        """
        Check if node has success/error edge routing configured.

        Returns:
            True if node has success or error type edges
        """
        for edge in self.edges:
            if edge.get("source") == self.node_id:
                edge_type = edge.get("type", "")
                if edge_type in ("success", "error"):
                    return True
        return False

    def _route_to_success_edge(self, result: Any) -> None:
        """
        When a node succeeds, any nodes connected via 'error' edges
        should be skipped.
        """

        # Filter all error edges from this node and skip their targets
        error_edges = self.filter_connected_edges(self.node_id, "error")

        # Skip all error path nodes at once
        for edge in error_edges:
            target_node_id = edge.get("target")
            if target_node_id:
                self.coordinator.mark_node_skipped(
                    target_node_id,
                    reason="error_edge_not_taken",
                    details={
                        "message": "Node succeeded, error path skipped",
                        "current_node": self.node_id,
                    },
                )

    def _handle_error_with_routing(self, error: Exception) -> None:
        """
        Handle node error with edge routing.

        When a node fails with error routing enabled:
        1. Store error context for downstream nodes
        2. Skip nodes on success edge
        3. Mark node as FAILED_HANDLED (not FAILED)
        4. Continue workflow on error path

        Args:
            error: Exception that occurred
        """
        import traceback

        error_context = {
            "message": str(error),
            "node_id": self.node_id,
            "timestamp": timezone.now().isoformat(),
            "trace": traceback.format_exc(),
            "type": type(error).__name__,
        }
        logger.debug(f"Storing error context for node {self.node_id}: {error_context}")
        # remove trace in context to reduce size or production
        error_context.pop("trace", None)
        # store error in context
        self.context.set_step(
            step_key=self.node_id,
            inputs=self._prepare_inputs(),
            outputs={"error": error_context},
            options={},
        )
        self.context.set_current("errors", error_context)

        # skip success path nodes
        success_edges = self.filter_connected_edges(self.node_id, "success")
        for edge in success_edges:
            target_node_id = edge.get("target")
            if target_node_id:
                self.coordinator.mark_node_skipped(
                    target_node_id,
                    reason="success_edge_not_taken",
                    details={
                        "message": "Node failed, error path taken, success path skipped",
                        "current_node": self.node_id,
                    },
                )

    def _create_run_node_record(self) -> None:
        """
        Create a WorkflowRunNode record in the database.
        """
        pass

    def _record_success(self, result: Any) -> None:
        """
        Record successful execution in the database.

        Args:
            result: Execution result
        """
        pass

    def _record_skipped(self, reason: Optional[str]) -> None:
        """
        Record skipped execution in the database.
        Args:
            reason: Reason for skipping
        """
        pass

    # ==================== Post-Execution Hook ====================
    def _post_execution(self, result: Dict[str, Any]) -> Any:
        """
        Post-execution tasks.

        Override in subclasses to implement node-specific logic:
        - ForkNodeExecutor: Mark non-selected branches as skipped
        - ConditionNodeExecutor: Mark false-branch nodes as skipped
        - LoopNodeExecutor: Set up loop variables for next iteration

        Args:
            result: Execution result
        """
        pass  # Default: no post-execution tasks

    # ==================== Utility Methods ====================

    # Utility Methods
    def filter_connected_edges(
        self, node_id: str, node_type: str
    ) -> List[Dict[str, Any]]:
        """
        Filter edges connected to this node by type.

        Args:
            node_id: ID of the current node
            node_type: Type of edge to filter ('success', 'error', etc.)

        Returns:
            List of edges matching the criteria
        """
        edge = [
            edge
            for edge in self.edges
            if edge.get("source") == node_id and edge.get("type") == node_type
        ]
        return edge

    def should_continue_on_error(self) -> bool:
        """
        Check if the node is configured to continue on error.

        Returns:
            True if continue_on_error is set, False otherwise
        """
        error_handling = self.node_config.get("error_handling", {})
        return error_handling.get("continue_on_error", False)

    def evaluate_config(self) -> Dict[str, Any]:  # type: ignore
        """
        Evaluate all expressions in node config.

        Replaces {{variable}} references with actual values using the template engine.

        Returns:
            Config with all expressions evaluated
        """
        # TODO: with template engine
        return self.context.evaluate_expression(self.node_config)

    def _prepare_inputs(self) -> Dict[str, Any]:
        """
        Prepare inputs dictionary for execute().

        Default implementation:
        - Evaluates all config expressions
        - Adds node metadata
        - Calls _get_additional_inputs() hook

        Subclasses can override for complete control,
        or just override _get_additional_inputs() to add custom data.

        Returns:
            Dictionary of inputs for execute()
        """
        # Evaluate all expressions in config
        evaluated_config = self.evaluate_config()

        # Build inputs dictionary
        inputs = {}
        inputs.update(self.node)
        inputs.update({"node_id": self.node_id, "node_type": self.node_type})
        inputs.update({"config": evaluated_config})

        # Allow subclasses to add custom inputs
        additional = self._get_additional_inputs()
        if additional:
            inputs.update(additional)

        return inputs

    def _get_additional_inputs(self) -> Dict[str, Any]:
        """
        Hook for subclasses to add custom inputs.

        Override this to provide node-specific data to execute().

        Example:
            def _get_additional_inputs(self):
                return {
                    'variables': self.context.get_all_variables(),
                    'previous_output': self.context.get_node_output('prev_node')
                }

        Returns:
            Additional inputs dictionary (default: empty)
        """
        return {}

    def execute_adapter(self) -> Any:
        """
        Execute the node's business logic.

        Subclasses MUST implement this method.
        Should return the node's output data.

        Returns:
            Output data from node execution

        Raises:
            Exception: If execution fails
        """
        inputs = self._prepare_inputs()
        return self.execute(inputs)

    @abstractmethod
    def execute(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Pure business logic - NO SIDE EFFECTS.

        Rules:
        - NO context access (use inputs parameter)
        - NO coordinator access (use inputs parameter)
        - NO database operations
        - NO logging (return data for logging instead)
        - Pure function: inputs → processing → outputs

        Benefits:
        - 100% unit testable
        - Reusable outside workflow
        - Can run independently
        - Composable

        Args:
            inputs: Dictionary containing all execution data
                {
                    'config': {...},      # Evaluated node configuration
                    'node_id': 'xyz',     # Node ID
                    'node_type': 'action',  # Node type
                    ... (additional inputs from _get_additional_inputs())
                }

        Returns:
            Execution result dictionary

        Raises:
            Exception: If execution fails

        Example:
            # In ActionNodeExecutor
            def execute(self, inputs):
                config = inputs['config']
                url = config['url']
                response = requests.get(url)
                return {'status': response.status_code, 'body': response.text}

            # Test it independently
            executor = ActionNodeExecutor(...)
            result = executor.execute({
                'config': {'url': 'https://api.com', 'method': 'GET'}
            })
            assert result['status'] == 200
        """
        pass

    def __str__(self):
        return f"{self.node_type.capitalize()}Executor({self.node_id})"

    def __repr__(self):
        return f"<{self.__class__.__name__}(node_id='{self.node_id}')>"
