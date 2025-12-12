"""
Main Workflow Executor

Orchestrates the execution of complete workflows.
Loads definition, validates, computes execution order, and runs nodes.
Pattern: Orchestrator
Responsibilities:
- Load workflow definition
- Validate structure
- Compute execution order
- Manage workflow-level state (WorkflowRun only)
- Coordinate node execution
Does NOT:
- Execute individual nodes
- Handle node-level errors
- Update WorkflowRunNode records
- Decide node skip logic
"""

import json
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

from django.utils import timezone

from workflow.algorithms.topological_sort import (
    compute_dependency_levels,
    group_nodes_by_level,
    topological_sort,
)
from workflow.engine.coordinator import ExecutionCoordinator
from workflow.engine.validators import validate_workflow_definition
from workflow.executors.registry import create_executor

logger = logging.getLogger(__name__)


class WorkflowExecutor:
    """
    Main workflow execution engine.
    Responsibilities:
    1. Load workflow definition
    2. Validate workflow structure
    3. Compute execution order (topological sort)
    4. Execute nodes in order
    5. Handle errors and retries
    6. Update run status
    """

    def __init__(self, run_id: str, workflow_definition: Dict[str, Any]):
        """
        Initialize executor.

        Args:
            run_id: UUID of the workflow run to execute
        """
        self.run_id = run_id
        self.run = None  # Placeholder for WorkflowRun object
        self.workflow_definition = workflow_definition
        self.nodes = workflow_definition.get("nodes", [])
        self.edges = workflow_definition.get("edges", [])
        self.execution_order: List[str] = []
        self.variables: Dict[str, Any] = self.workflow_definition.get("variables", {})
        self.coordinator = ExecutionCoordinator(run_id, self.variables)
        self.context = self.coordinator.context
        self._failed_node_id: Optional[str] = None  # Track which node failed
        # Workflow-level timeout configuration (in seconds)
        self.workflow_timeout = workflow_definition.get("config", {}).get(
            "timeout", None
        )
        self.level_timeout = workflow_definition.get("config", {}).get(
            "level_timeout", 86400
        )  # 24 hours default for long-running datacenter operations

    def _initialize_coordinator(self):
        """
        Initialize the execution coordinator with workflow context.
        """
        self.coordinator.context.set_variables(self.variables)
        # Store executor instance in private internal storage (not exposed to users)
        self.coordinator.context._set_workflow_executor(self)
        self.coordinator.context.set_metadatas(
            {
                "run_id": self.run_id,
                "started_at": timezone.now(),
            }
        )
        # Initialize system data with auto-generated fields
        now = timezone.now()
        self.coordinator.context.update_system(
            {
                "timestamp": now.isoformat() + "Z",
                "timestamp_ms": int(now.timestamp() * 1000),
                "date": now.strftime("%Y-%m-%d"),
                "time": now.strftime("%H:%M:%S"),
                "uuid": str(uuid.uuid4()),
                "run_id": self.run_id,
                "workflow_id": self.workflow_definition.get("id"),
                "workflow_name": self.workflow_definition.get("name"),
                "workflow_version": self.workflow_definition.get("version", "1.0.0"),
                "user": self.workflow_definition.get("user", "system"),
                "total_nodes": len(self.nodes),
                "total_edges": len(self.edges),
                "execution_mode": "parallel",
                "timezone": str(timezone.get_current_timezone()),
                "created_at": now,
                "updated_at": now,
            }
        )

    def _compute_execution_order(self):
        self.execution_order = topological_sort(self.nodes, self.edges)
        print(f"Computed execution order: {self.execution_order}")

    # ==================== Execution Phase ====================

    def execute(self) -> Dict[str, Any]:
        """
        Execute the workflow.

        Returns:
            A dictionary with execution results and status.
        """
        try:
            # Initialize coordinator with variables
            self._initialize_coordinator()
            # Validate workflow definition
            self._validate_workflow()
            # Compute execution order
            self._compute_execution_order()
            # Execute nodes in order
            self._execute_nodes()

            self._complete_workflow()
            return {
                "status": "completed",
                "execution_order": self.execution_order,
            }
        except Exception as e:
            logger.error(f"Workflow execution failed: {e}")
            return self._format_error_response(e)

    def _validate_workflow(self):
        is_valid, errors, warnings = validate_workflow_definition(
            self.workflow_definition
        )
        if not is_valid:
            raise ValueError({"errors": errors, "msg": "Workflow validation failed."})
        if warnings:
            logger.warning(f"Workflow validation warnings: {warnings}")

    def _execute_nodes(self):
        """
        Execute nodes in parallel by dependency level.

        Computes dependency levels and executes all nodes at the same level
        concurrently using ThreadPoolExecutor.

        """
        levels = compute_dependency_levels(self.nodes, self.edges)
        nodes_by_level = group_nodes_by_level(self.nodes, levels)
        print(f"Executing workflow in {len(nodes_by_level)} levels...")

        nodes_dict = {node["id"]: node for node in self.nodes}
        # print(f"loaded nodes dict: {nodes_dict}")

        for level_num, level_nodes in enumerate(nodes_by_level):

            node_ids = [n["id"] for n in level_nodes]
            print(f"\n--- Executing Level {level_num} with nodes: {node_ids} ---")

            # Execute all nodes in this level concurrently
            self._execute_level_parallel(level_nodes, nodes_dict)
            # Check if workflow should halt (circuit breaker)
            should_halt, halt_reason = self.coordinator.should_halt()

            if should_halt:
                logger.info(f"Halting workflow execution: {halt_reason}")
                break

    def _execute_level_parallel(
        self, level_nodes: List[Dict[str, Any]], nodes_dict: Dict[str, Dict[str, Any]]
    ):
        """
        Execute all nodes in a given level in parallel.

        Args:
            level_nodes: List of node configurations at the current level
            nodes_dict: Dictionary mapping node_id to node configuration
        """
        if len(level_nodes) == 1:
            # Optimization: single node, execute directly (no threading overhead)
            node = level_nodes[0]
            node_id = node["id"]
            is_skipped, skip_info = self.coordinator.is_node_skipped(node_id)
            if not is_skipped:
                self._execute_single_node(node)
            else:
                logger.info(f"Skipping node {node_id}: {skip_info}")
            return

        # Use ThreadPoolExecutor to execute nodes in parallel
        # Cap at 10 concurrent threads to avoid overload
        max_workers = min(10, len(level_nodes))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            #  Submit ALL nodes (batch)
            futures = {}
            for node in level_nodes:
                node_id = node["id"]
                is_skipped, skip_info = self.coordinator.is_node_skipped(node_id)
                if is_skipped:
                    logger.info(f"Skipping node {node_id}: {skip_info}")
                    continue

                # submit node execution to thread pool
                future = executor.submit(self._execute_single_node, node)
                futures[future] = node_id

            # wait for all futures to complete with timeout
            try:
                for future in as_completed(futures, timeout=self.level_timeout):
                    node_id = futures[future]
                    try:
                        future.result()  # Raises exception if node failed
                        logger.debug(f"Node {node_id} completed successfully")
                    except Exception as e:
                        # Exception already handled by _execute_single_node
                        # We only re-raise if workflow should fail
                        logger.error(f"Node {node_id} execution failed: {e}")
                        node = nodes_dict[node_id]

                        if (
                            not node.get("config", {})
                            .get("error_handling", {})
                            .get("continue_on_error", False)
                        ):
                            logger.error(f"Node {node_id} failed, workflow will fail")
                            # Don't raise immediately - let other threads finish
                            # The exception will be re-raised from _execute_single_node
                        else:
                            logger.warning(
                                f"Node {node_id} failed but workflow continuing"
                            )
            except TimeoutError:
                logger.error(
                    f"Workflow level execution timeout ({self.level_timeout}s) exceeded"
                )
                # Cancel remaining futures
                for future in futures:
                    future.cancel()
                raise TimeoutError(
                    f"Workflow level execution timeout ({self.level_timeout}s) exceeded"
                )

    def _execute_single_node(self, node: Dict[str, Any]):
        """
        Execute a single node by delegating to NodeExecutor.

        NodeExecutor is fully responsible for:
        - Skip logic
        - Execution
        - Error handling
        - Retries
        - Database updates
        - Post-execution tasks

        Args:
            node: Node configuration
        """
        node_id = node["id"]
        node_type = node["type"]
        print(f"Executing node {node_id} of type {node_type}")

        try:
            executor = create_executor(node, self.coordinator)
            # Let executor handle full lifecycle
            executor.run()
        except Exception as e:
            # Node executor will have already handled error logging and DB updates
            # We only catch here to handle workflow-level failure
            config = node.get("config", {})

            if not config.get("error_handling", {}).get("continue_on_error", False):
                # Node failed and workflow should fail
                logger.error(f"Workflow failed due to node {node_id}: {e}")
                # Store the failed node_id for error reporting
                self._failed_node_id = node_id
                raise e
            else:
                # Node failed but workflow continues
                # ExecutionCoordinator and NodeExecutor have already handled routing
                logger.warning(f"Node {node_id} failed but workflow continuing")

    # Resume and Pause
    # def resume(self) -> Dict[str, Any]:
    #     """
    #     Resume paused workflow from where it left off.

    #     Used after human task completion or manual resume.
    #     Continues execution from the node after the paused one.

    #     Returns:
    #         Execution result

    #     Raises:
    #         ValueError: If workflow cannot be resumed
    #     """
    #     logger.info(f"Resuming workflow run {self.run_id}")

    #     try:
    #         # Initialize coordinator with variables
    #         self._initialize_coordinator()
    #         # Validate workflow definition
    #         self._validate_workflow()
    #         # Compute execution order
    #         self._compute_execution_order()
    #         # Varify workflow is in paused state
    #         # TODO: for now use context manager letter use DB check
    #         if

    # ==================== Workflow-Level State Management ====================

    def _mark_workflow_running(self):
        """Mark workflow as running (WorkflowRun table only)"""
        # TODO: Implement actual DB update logic
        logger.debug(f"Workflow marked as RUNNING")

    def _mark_workflow_complete(self):
        # TODO: Implement actual DB update logic
        logger.debug(f"Workflow marked as SUCCESS")

    def _mark_workflow_failed(self, error: Exception):
        """
        Mark workflow as failed.

        Args:
            error: Exception that caused failure
        """
        # TODO: Implement actual DB update logic
        logger.debug(f"Workflow marked as FAILED: {error}")

    def _complete_workflow(self):
        completed_at = timezone.now()
        print(f"Workflow completed at: {completed_at}")
        self.coordinator.context.set_metadata("completed_at", completed_at)
        return {
            "completed_at": completed_at,
        }

    def _format_error_response(
        self, error: Exception, node_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Format error response in a structured way.

        Args:
            error: Exception that occurred
            node_id: Optional node ID where error occurred

        Returns:
            Structured error dictionary
        """
        import traceback

        error_type = type(error).__name__
        error_message = str(error)

        # Log full traceback for debugging
        logger.error(f"Full traceback:\n{traceback.format_exc()}")

        # Use stored failed node_id if available
        if not node_id and hasattr(self, "_failed_node_id"):
            node_id = self._failed_node_id

        # Try to extract node_id from coordinator events if still not provided
        if not node_id:
            events = self.coordinator.get_execution_events()
            for event in reversed(events):
                if event.get("event_type") == "node_failed":
                    node_id = event.get("node_id")
                    break

        error_response = {
            "status": "failed",
            "error": {
                "type": error_type,
                "message": error_message,
                "node_id": node_id,
                "timestamp": str(timezone.now()),
            },
            "execution_order": self.execution_order,
            "completed_nodes": [
                node_id
                for node_id in self.execution_order
                if self.coordinator.get_node_state(node_id) == "success"
            ],
        }

        return error_response
