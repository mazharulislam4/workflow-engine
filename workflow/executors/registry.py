"""
Node Executor Registry

Manages registration and creation of node executors.
Uses Factory and Registry patterns.
"""

from typing import Any, Callable, Dict, Type

from workflow.engine.context import ContextManager
from workflow.engine.coordinator import ExecutionCoordinator
from workflow.executors.base import NodeExecutor


class NodeExecutorRegistry:
    """
    Registry for node executors.

    Maps node types to their executor classes.
    """

    def __init__(self):
        self._executors: Dict[str, Type[NodeExecutor]] = {}

    def register_executor(
        self, node_type: str, executor_cls: Type[NodeExecutor]
    ) -> None:
        """
        Register a node executor class for a given node type.

        Args:
            node_type: The type of the node
            executor_cls: The executor class to register
        """
        self._executors[node_type] = executor_cls

    def get(self, node_type: str) -> Type[NodeExecutor]:
        """
        Get the executor class for a given node type.

        Args:
            node_type: The type of the node
        Returns:
            The executor class for the node type
        """
        executor_cls = self._executors.get(node_type)
        if not executor_cls:
            raise ValueError(f"No executor registered for node type '{node_type}'")
        return executor_cls

    def is_registered(self, node_type: str) -> bool:
        """
        Check if an executor is registered for a given node type.

        Args:
            node_type: The type of the node
        Returns:
            True if an executor is registered for the node type, False otherwise
        """
        return node_type in self._executors

    def get_all_types(self) -> list[str]:
        """
        Get all registered node types and their executor classes.

        Returns:
            Dict mapping node types to executor classes
        """
        return list(self._executors.keys())


# Global registry instance
_registry = NodeExecutorRegistry()


def register_executor(node_type: str):
    """
    Decorator to register a node executor.

    Usage:
        @register_executor('action')
        class ActionNodeExecutor(NodeExecutor):
            ...
    """

    def decorator(executor_cls: Type[NodeExecutor]) -> Type[NodeExecutor]:
        _registry.register_executor(node_type, executor_cls)
        return executor_cls

    return decorator


def get_executor_class(node_type: str) -> Type[NodeExecutor]:
    """
    Get the executor class for a given node type.

    Args:
        node_type: The type of the node
    Returns:
        The executor class for the node type
    """
    return _registry.get(node_type)


def create_executor(node: dict, coordinator: ExecutionCoordinator) -> NodeExecutor:
    """
    Create executor instance for a node.

    Args:
        node: Node configuration
        coordinator: Execution coordinator for communication

    Returns:
        Node executor instance

    Raises:
        ValueError: If node has no type
        KeyError: If node type not registered
    """
    node_type = node.get("type")
    if not node_type:
        raise ValueError(f"Node {node.get('id')} has no type")

    executor_class = get_executor_class(node_type)
    return executor_class(node, coordinator)


def is_registered(node_type: str) -> bool:
    """Check if node type is registered"""
    return _registry.is_registered(node_type)


def get_all_node_types() -> list[str]:
    """Get all registered node types"""
    return _registry.get_all_types()
