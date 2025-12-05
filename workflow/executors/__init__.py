"""
Executors Package

Import all executors to trigger registration.
"""

from workflow.executors import condition  # noqa: F401
from workflow.executors import end  # noqa: F401
from workflow.executors import start  # noqa: F401

# Import all executors to register them
from workflow.executors.condition import ConditionNodeExecutor
from workflow.executors.end import EndNodeExecutor
from workflow.executors.http_request import HTTPRequestExecutor

# Import registry for external use
from workflow.executors.registry import (
    create_executor,
    get_all_node_types,
    get_executor_class,
    is_registered,
    register_executor,
)
from workflow.executors.start import StartNodeExecutor

__all__ = [
    "StartNodeExecutor",
    "EndNodeExecutor",
    "ConditionNodeExecutor",
    "HTTPRequestExecutor",
    # Registry functions
    "register_executor",
    "get_executor_class",
    "is_registered",
    "get_all_node_types",
    "create_executor",
]
