from copy import deepcopy
from logging import getLogger
from threading import RLock
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from workflow.engine.executor import WorkflowExecutor

logger = getLogger(__name__)


class ContextManager:
    """Manages context data for workflow execution."""

    def __init__(self) -> None:
        self.state = {
            "variables": {},
            # each step's state exp: { step1: {outputs: {}, input: {}} }
            "steps": {},
            # store data by lookup key  {[step_id]: {inputs: {}, outputs: {}, options: {}}}
            "lookup": {},
            # initial inputs
            "inputs": {},
            "loop": {},
            # final outputs
            "outputs": {},
            "metadata": {},
            "current": {},
            "system": {},
        }
        self._lock = RLock()
        # Private internal storage (not exposed in state)
        self._internal = {}

    # Get workflow executor instance
    def get_workflow_executor(self) -> Optional["WorkflowExecutor"]:
        """Get the workflow executor instance (stored privately, not in user-accessible metadata)."""
        with self._lock:
            return self._internal.get("workflow_executor")

    # Set workflow executor instance (internal only)
    def _set_workflow_executor(self, executor: "WorkflowExecutor") -> None:
        """Set the workflow executor instance in private storage."""
        with self._lock:
            self._internal["workflow_executor"] = executor

    # set variables
    def set_variable(self, key: str, value: Any) -> None:
        with self._lock:
            self.state["variables"][key] = value
        return self.state["variables"][key]

    # set multiple variables
    def set_variables(self, variables: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            for key, value in variables.items():
                self.state["variables"][key] = value
        return self.state["variables"]

    # update variables
    def update_variables(self, key: str, variables: Dict[str, Any]) -> None:
        with self._lock:
            if key not in self.state["variables"]:
                return None
            self.state["variables"][key].update(variables)
        return self.state["variables"][key]

    # get variables
    def get_variable(self, key: str) -> Any:
        with self._lock:
            if key not in self.state["variables"]:
                return None
            return self.state["variables"].get(key)

    # delete a variable
    def delete_variable(self, key: str) -> Any:
        with self._lock:
            if key not in self.state["variables"]:
                return None
            return self.state["variables"].pop(key, None)

    # clear variables
    def clear_variables(self) -> Any:
        with self._lock:
            self.state["variables"].clear()
        return self.state["variables"]

    # set step data
    def set_step(
        self,
        step_key: str,
        inputs: Dict[str, Any],
        outputs: Dict[str, Any],
        options: Dict[str, Any],
    ) -> Any:
        """
        Save the task step information
        """

        with self._lock:
            if not self.has_step(step_key):
                self.state["steps"][step_key] = {
                    "inputs": inputs or {},
                    "outputs": outputs or {},
                    "options": options or {},
                }
                logger.debug(
                    f"Step '{step_key}' set with inputs: {inputs}, outputs: {outputs}, options: {options}"
                )
        return deepcopy(self.state["steps"][step_key])

    # check if step exists
    def has_step(self, step_key: str) -> bool:
        with self._lock:
            return step_key in self.state["steps"]

    # get step data
    def get_step(self, step_key: str) -> Any:
        with self._lock:
            return deepcopy(self.state["steps"].get(step_key, {}))

    # update step data
    def update_step(
        self,
        step_key: str,
        inputs: Optional[Dict[str, Any]] = None,
        outputs: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> Any:
        with self._lock:
            if not self.has_step(step_key):
                return None
            if inputs:
                self.state["steps"][step_key]["inputs"].update(inputs)
            if outputs:
                self.state["steps"][step_key]["outputs"].update(outputs)
            if options:
                self.state["steps"][step_key]["options"].update(options)
        return deepcopy(self.state["steps"][step_key])

    # clear all steps
    def clear_steps(self) -> Any:
        with self._lock:
            self.state["steps"].clear()
        return self.state["steps"]

    # delete a step
    def delete_step(self, step_key: str) -> None:
        with self._lock:
            if not self.has_step(step_key):
                return None
            return self.state["steps"].pop(step_key, None)

    # set lookup data
    def set_lookup(self, lookup_key: str, data: Dict[str, Any]) -> Any:
        """
        Store data by lookup key {[step_id]: {inputs: {}, outputs: {}, options: {}}}
        """
        with self._lock:
            if not lookup_key:
                return None
            self.state["lookup"][lookup_key] = data
        return deepcopy(self.state["lookup"][lookup_key])

    # get lookup data
    def get_lookup(self, lookup_key: str) -> Any:
        with self._lock:
            return deepcopy(self.state["lookup"].get(lookup_key, {}))

    def get_lookups(self, lookup_keys: List[str]) -> Dict[str, Any]:
        with self._lock:
            return {
                key: deepcopy(self.state["lookup"].get(key, {})) for key in lookup_keys
            }

    # update lookup data
    def update_lookup(self, lookup_key: str, data: Dict[str, Any]) -> Any:
        with self._lock:
            if lookup_key not in self.state["lookup"]:
                return None

            if isinstance(self.state["lookup"][lookup_key], dict) and isinstance(
                data, dict
            ):
                self.state["lookup"][lookup_key].update(data)
            else:
                self.state["lookup"][lookup_key] = data
        return deepcopy(self.state["lookup"][lookup_key])

    # delete lookup data
    def delete_lookup(self, lookup_key: str) -> None:
        with self._lock:
            if lookup_key not in self.state["lookup"]:
                return None
            return self.state["lookup"].pop(lookup_key, None)

    # set inputs
    def set_inputs(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Set initial workflow inputs."""
        with self._lock:
            self.state["inputs"] = inputs
        return deepcopy(self.state["inputs"])

    # get inputs
    def get_inputs(self) -> Dict[str, Any]:
        """Get initial workflow inputs."""
        with self._lock:
            return deepcopy(self.state["inputs"])

    # update inputs
    def update_inputs(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Update initial workflow inputs."""
        with self._lock:
            self.state["inputs"].update(inputs)
        return deepcopy(self.state["inputs"])

    # clear inputs
    def clear_inputs(self) -> Dict[str, Any]:
        """Clear all initial workflow inputs."""
        with self._lock:
            self.state["inputs"].clear()
        return self.state["inputs"]

    # set loop
    def set_loop(self, key: str, value: Any) -> Any:
        """Set loop data."""
        with self._lock:
            self.state["loop"][key] = value
        return self.state["loop"][key]

    # get loop
    def get_loop(self, key: Optional[str] = None) -> Any:
        """Get loop data. If key is None, returns all loop data."""
        with self._lock:
            if key is None:
                return deepcopy(self.state["loop"])
            return self.state["loop"].get(key)

    # update loop
    def update_loop(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Update loop data."""
        with self._lock:
            self.state["loop"].update(data)
        return deepcopy(self.state["loop"])

    # clear loop
    def clear_loop(self) -> Dict[str, Any]:
        """Clear all loop data."""
        with self._lock:
            self.state["loop"].clear()
        return self.state["loop"]

    # set outputs
    def set_outputs(self, outputs: Dict[str, Any]) -> Dict[str, Any]:
        """Set final workflow outputs."""
        with self._lock:
            self.state["outputs"] = outputs
        return deepcopy(self.state["outputs"])

    # get outputs
    def get_outputs(self) -> Dict[str, Any]:
        """Get final workflow outputs."""
        with self._lock:
            return deepcopy(self.state["outputs"])

    # update outputs
    def update_outputs(self, outputs: Dict[str, Any]) -> Dict[str, Any]:
        """Update final workflow outputs."""
        with self._lock:
            self.state["outputs"].update(outputs)
        return deepcopy(self.state["outputs"])

    # clear outputs
    def clear_outputs(self) -> Dict[str, Any]:
        """Clear all final workflow outputs."""
        with self._lock:
            self.state["outputs"].clear()
        return self.state["outputs"]

    # set metadata
    def set_metadatas(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Set workflow metadata."""
        with self._lock:
            # update multiple metadata entries
            for key, value in metadata.items():
                self.state["metadata"][key] = value
        return deepcopy(self.state["metadata"])

    # set metadata
    def set_metadata(self, key: str, value: Any) -> Any:
        """Set workflow metadata."""
        with self._lock:
            self.state["metadata"][key] = value
        return self.state["metadata"][key]

    # get metadata
    def get_metadata(self) -> Dict[str, Any]:
        """Get workflow metadata."""
        with self._lock:
            return deepcopy(self.state["metadata"])

    # update metadata
    def update_metadata(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Update workflow metadata."""
        with self._lock:
            self.state["metadata"].update(metadata)
        return deepcopy(self.state["metadata"])

    # clear metadata
    def clear_metadata(self) -> Dict[str, Any]:
        """Clear all workflow metadata."""
        with self._lock:
            self.state["metadata"].clear()
        return self.state["metadata"]

    # set current
    def set_current(self, key: str, value: Any) -> Any:
        """Set current execution context data."""
        with self._lock:
            self.state["current"][key] = value
        return self.state["current"][key]

    # get current
    def get_current(self, key: Optional[str] = None) -> Any:
        """Get current execution context data. If key is None, returns all current data."""
        with self._lock:
            if key is None:
                return deepcopy(self.state["current"])
            return self.state["current"].get(key)

    # update current
    def update_current(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Update current execution context data."""
        with self._lock:
            self.state["current"].update(data)
        return deepcopy(self.state["current"])

    # clear current
    def clear_current(self) -> Dict[str, Any]:
        """Clear all current execution context data."""
        with self._lock:
            self.state["current"].clear()
        return self.state["current"]

    # set system
    def set_system(self, key: str, value: Any) -> Any:
        """Set system-level data."""
        with self._lock:
            self.state["system"][key] = value
        return self.state["system"][key]

    # get system
    def get_system(self, key: Optional[str] = None) -> Any:
        """Get system-level data. If key is None, returns all system data."""
        with self._lock:
            if key is None:
                return deepcopy(self.state["system"])
            return self.state["system"].get(key)

    # update system
    def update_system(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Update system-level data."""
        with self._lock:
            self.state["system"].update(data)
        return deepcopy(self.state["system"])

    # clear system
    def clear_system(self) -> Dict[str, Any]:
        """Clear all system-level data."""
        with self._lock:
            self.state["system"].clear()
        return self.state["system"]

    # clear all context
    def clear_all(self) -> Dict[str, Any]:
        """Clear all workflow context."""
        with self._lock:
            self.state["variables"].clear()
            self.state["steps"].clear()
            self.state["lookup"].clear()
            self.state["inputs"].clear()
            self.state["loop"].clear()
            self.state["outputs"].clear()
            self.state["metadata"].clear()
            self.state["current"].clear()
            self.state["system"].clear()
        return self.state

    def get_state(self) -> Dict[str, Any]:
        """Get the entire context state."""
        with self._lock:
            return deepcopy(self.state)

    def set_state(self, state: Dict[str, Any]) -> None:
        """Set the entire context state."""
        with self._lock:
            self.state = deepcopy(state)

    def evaluate_expression(self, expression: Any) -> Any:
        """
        Evaluate a Jinja2 expression against the current context state.
        Handles:
        - Strings: "Hello {{nodes.user.output.name}}"
        - Dicts: {"key": "{{value}}"}
        - Lists: ["{{item1}}", "{{item2}}"]
        - Pure values: 123, True, None (returned as-is)

        Args:
            expression: The expression to evaluate (string, dict, list, or any value)

        Returns:
            The evaluated result

        Raises:
            ValueError: If template rendering fails
        """
        from workflow.engine.template_engine import TemplateEngine

        engine = TemplateEngine()

        # Get current context state for template rendering
        with self._lock:
            context = deepcopy(self.state)

        # Use the template engine to render the expression
        # The render_data_structure method handles strings, dicts, lists recursively
        try:
            return engine.render_data_structure(expression, context)
        except ValueError as e:
            logger.error(f"Failed to evaluate expression: {e}")
            raise

    def get_all(self) -> Dict[str, Any]:
        """
        Get all context data including variables, steps, loop state, skipped nodes, etc.

        Returns:
            Dictionary containing complete context state
        """
        with self._lock:
            # Get skipped nodes from internal storage
            skipped_nodes = self._internal.get("skipped_nodes", {})

            # Return state with skipped nodes added
            result = deepcopy(self.state)
            result["skipped_nodes"] = skipped_nodes
            return result


ContextManagerInstance = ContextManager()
