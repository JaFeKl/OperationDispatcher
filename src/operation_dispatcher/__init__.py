from .models import (
    DependencyType,
    DispatchEvent,
    EventType,
    ExecutionState,
    ExecutionOutcome,
    OperationDependency,
    OperationHistoryEntry,
    OperationExecution,
    OperationDispatcherState,
    ScheduledOperation,
    TerminationReason,
)
from .dispatch_queue import DispatchQueue, SortDirection, SortField, SortRule
from .operation_dispatcher import OperationDispatcher
from .operation_dispatcher_openapi import OperationDispatcherOpenAPI
from .utils.simulated_operation_runner import SimulatedOperationRunner

try:
    from .operation_dispatcher_mcp import (
        OperationDispatcherMCPContext,
        OperationDispatcherMCPServer,
        create_operation_dispatcher_mcp_server,
    )
except ImportError:
    pass

_OPTIONAL_MCP_EXPORTS = {
    "OperationDispatcherMCPContext",
    "OperationDispatcherMCPServer",
    "create_operation_dispatcher_mcp_server",
}

__all__ = [
    "DependencyType",
    "ScheduledOperation",
    "OperationDependency",
    "OperationHistoryEntry",
    "OperationExecution",
    "ExecutionState",
    "ExecutionOutcome",
    "TerminationReason",
    "DispatchEvent",
    "EventType",
    "OperationDispatcherState",
    "DispatchQueue",
    "SortField",
    "SortDirection",
    "SortRule",
    "OperationDispatcher",
    "OperationDispatcherOpenAPI",
    "SimulatedOperationRunner",
]

if "OperationDispatcherMCPServer" in globals():
    __all__.extend(
        [
            "OperationDispatcherMCPContext",
            "OperationDispatcherMCPServer",
            "create_operation_dispatcher_mcp_server",
        ]
    )


def __getattr__(name: str):
    if name in _OPTIONAL_MCP_EXPORTS:
        raise ImportError(
            "MCP support is optional. Install with `operation-dispatcher[mcp]` "
            "to use MCP exports."
        )
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
