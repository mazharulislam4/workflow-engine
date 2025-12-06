"""
Executors Package

Import all executors to trigger registration.
"""

from workflow.executors import condition  # noqa: F401
from workflow.executors import end  # noqa: F401
from workflow.executors import fork  # noqa: F401
from workflow.executors import join  # noqa: F401
from workflow.executors import loop  # noqa: F401
from workflow.executors import parallel  # noqa: F401
from workflow.executors import path  # noqa: F401
from workflow.executors import start  # noqa: F401

# Import all executors to register them
from workflow.executors.condition import ConditionNodeExecutor
from workflow.executors.end import EndNodeExecutor
from workflow.executors.fork import ForkNodeExecutor
from workflow.executors.http_request import HTTPRequestExecutor
from workflow.executors.join import JoinNodeExecutor
from workflow.executors.loop import LoopNodeExecutor
from workflow.executors.parallel import ParallelNodeExecutor
from workflow.executors.path import PathNodeExecutor

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
    "ForkNodeExecutor",
    "JoinNodeExecutor",
    "PathNodeExecutor",
    "HTTPRequestExecutor",
    # Registry functions
    "register_executor",
    "get_executor_class",
    "is_registered",
    "get_all_node_types",
    "create_executor",
]
