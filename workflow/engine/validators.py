"""
Workflow Validators
"""

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

from workflow.utils.constants import REQUIRED_EDGE_FIELDS as REQUIRED_EDGE_FIELDS_LIST
from workflow.utils.constants import REQUIRED_NODE_FIELDS as REQUIRED_NODE_FIELDS_LIST
from workflow.utils.constants import (
    REQUIRED_WORKFLOW_FIELDS as REQUIRED_WORKFLOW_FIELDS_SET,
)
from workflow.utils.constants import VALID_NODE_TYPES as VALID_NODE_TYPES_LIST

# Add project root to path for standalone execution
if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(project_root))

from workflow.algorithms.cycle_detection import find_cycle_path, has_cycle
from workflow.algorithms.reachability import find_unreachable_nodes


class WorkflowValidationError(Exception):
    """Custom exception for workflow validation errors."""

    pass


class WorkflowValidator:
    """
    Validates workflow structure before execution.
    checks:
    - Required fields exist
    - No cycles in the workflow DAG
    - Valid node types
    - All nodes are reachable from the start node
    - Edges reference valid nodes
    """

    def __init__(self, workflow_data: Dict[str, Any]):
        self.workflow_data = workflow_data
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.REQUIRED_WORKFLOW_FIELDS = REQUIRED_WORKFLOW_FIELDS_SET
        self.REQUIRED_NODE_FIELDS = REQUIRED_NODE_FIELDS_LIST
        self.REQUIRED_EDGE_FIELDS = REQUIRED_EDGE_FIELDS_LIST
        self.VALID_NODE_TYPES = VALID_NODE_TYPES_LIST

    def is_valid(self) -> bool:
        """Run all validations and return True if valid, else False."""
        self.errors.clear()
        self.warnings.clear()

        self._validate_required_fields()
        self._validate_nodes()
        self._validate_edges()
        self._validate_no_cycles()
        self._validate_reachability()

        return len(self.errors) == 0

    def get_errors(self) -> List[str]:
        """Return list of validation errors."""
        return self.errors

    def get_warnings(self) -> List[str]:
        """Return list of validation warnings."""
        return self.warnings

    def _validate_required_fields(self):
        """Validate all top-level required fields exist."""
        for field in self.REQUIRED_WORKFLOW_FIELDS:
            if field not in self.workflow_data:
                self.errors.append(f"Missing required workflow field: {field}")

    def _validate_nodes(self):
        """Validate all nodes"""
        nodes = self.workflow_data.get("nodes", [])

        if not nodes:
            self.errors.append("Workflow must contain at least one node.")
            return

        node_ids = set()

        for i, node in enumerate(nodes):

            # check required fields
            for field in self.REQUIRED_NODE_FIELDS:
                if field not in node:
                    self.errors.append(f"Node {i} missing required field: {field}")
            # check duplicate IDs
            node_id = node.get("id")
            if node_id in node_ids:
                self.errors.append(f"Duplicate node ID found: {node_id}")
            else:
                node_ids.add(node_id)
                # validate node type
            node_type = node.get("type")
            if node_type not in self.VALID_NODE_TYPES:
                self.errors.append(
                    f"Node {node_id} has invalid type: {node_type}. Valid types are: {self.VALID_NODE_TYPES}"
                )
            # validate node has config
            if "config" not in node:
                self.warnings.append(f"Node {node_id} has no config defined.")

    def _validate_edges(self):
        """Validate all edges"""
        edges = self.workflow_data.get("edges", [])
        nodes = self.workflow_data.get("nodes", [])
        node_ids = {node["id"] for node in nodes}

        for i, edge in enumerate(edges):
            # check required fields
            for field in self.REQUIRED_EDGE_FIELDS:
                if field not in edge:
                    self.errors.append(f"Edge {i} missing required field: {field}")

            source = edge.get("source")
            target = edge.get("target")

            if source not in node_ids:
                self.errors.append(f"Edge {i} has invalid source node ID: {source}")
            if target not in node_ids:
                self.errors.append(f"Edge {i} has invalid target node ID: {target}")

            # check for self-loops
            if source == target:
                self.errors.append(f"Edge {i} forms a self-loop on node ID: {source}")

    def _validate_no_cycles(self):
        """Validate the workflow graph has no cycles."""
        nodes = self.workflow_data.get("nodes", [])
        edges = self.workflow_data.get("edges", [])

        has_cycle_flag, cycle_path = has_cycle(nodes, edges)
        if has_cycle_flag:
            if cycle_path:
                cycle_str = " -> ".join(cycle_path)
                self.errors.append(f"Workflow contains a cycle: {cycle_str}")
            else:
                self.errors.append("Workflow contains a cycle.")

    def _validate_reachability(self):
        """Validate all nodes are reachable from the start node."""
        nodes = self.workflow_data.get("nodes", [])
        edges = self.workflow_data.get("edges", [])
        if not nodes or not edges:
            return
        start_nodes = [
            node
            for node in nodes
            if node.get("type") == "start" or node.get("type") == "trigger"
        ]
        if not start_nodes:
            self.errors.append("Workflow must have a start node.")
            return

        start_node_id = start_nodes[0]["id"]

        try:
            unreachable_nodes = find_unreachable_nodes(nodes, edges, start_node_id)
            if unreachable_nodes:
                unreachable_str = ", ".join(unreachable_nodes)
                self.errors.append(
                    f"The following nodes are unreachable from the start node '{start_node_id}': {unreachable_str}"
                )
        except ValueError as e:
            self.errors.append(f"Error during reachability validation: {e}")

    def validate_start_node(self):
        """Validate there is exactly one start node. start node should be only one."""
        nodes = self.workflow_data.get("nodes", [])
        start_nodes = [
            node
            for node in nodes
            if node.get("type") == "start" or node.get("type") == "trigger"
        ]

        if len(start_nodes) == 0:
            self.errors.append("Workflow must have one start node.")
        elif len(start_nodes) > 1:
            self.errors.append("Workflow must not have more than one start node.")

            # start node should not be incoming edges
        edges = self.workflow_data.get("edges", [])
        start_node_ids = {node["id"] for node in start_nodes}
        for edge in edges:
            if edge["target"] in start_node_ids:
                self.errors.append(
                    f"Start node '{edge['target']}' should not have incoming edges."
                )


def validate_workflow_definition(
    workflow_definition: Dict[str, Any],
) -> Tuple[bool, List[str], List[str]]:
    """
    Validate the given workflow definition.

    Args:
        workflow_definition: The workflow definition as a dictionary.

    Returns:
        A tuple (is_valid, errors, warnings)
    """
    validator = WorkflowValidator(workflow_definition)
    is_valid = validator.is_valid()
    errors = validator.get_errors()
    warnings = validator.get_warnings()
    return is_valid, errors, warnings


# example test
if __name__ == "__main__":
    # Example workflow definition
    workflow_def = {
        "id": "workflow_1",
        "name": "Sample Workflow",
        "nodes": [
            {"id": "start", "type": "start", "name": "Start Node"},
            {"id": "a", "type": "action", "name": "Action A"},
            {"id": "b", "type": "action", "name": "Action B"},
            {"id": "end", "type": "end", "name": "End Node"},
        ],
        "edges": [
            {"source": "start", "target": "a"},
            {"source": "a", "target": "b"},
            {"source": "b", "target": "end"},
        ],
    }

    is_valid, errors, warnings = validate_workflow_definition(workflow_def)

    if is_valid:
        print("Workflow is valid.")
    elif warnings:
        print("Workflow is valid with warnings:")
        for warning in warnings:
            print(f"- {warning}")
    else:
        print("Workflow is invalid. Errors:")
        for error in errors:
            print(f"- {error}")
