from .models import (
    DependencyType,
    DispatchEvent,
    EventType,
    ExecutionState,
    ExecutionOutcome,
    History,
    OperationDependency,
    OperationDispatcherState,
    Operation,
    TerminationReason,
)
from .dispatch_queue import DispatchQueue, SortDirection, SortField, SortRule
from .operation_dispatcher import OperationDispatcher
from .runtime_controller import OperationDispatcherRuntimeController
from .services import (
    DispatcherHistoryService,
    DispatcherRuntimeService,
    DispatcherStateStore,
    OperationLifecycleService,
)
from .utils.simulated_operation_runner import SimulatedOperationRunner
from .visualization import (
    BrowserEventVisualizer,
    build_history_gantt_figure,
    show_history_gantt,
)
from .diagnostics.history_analyzer import HistoryAnalyzer

try:
    from .operation_dispatcher_openapi import OperationDispatcherOpenAPI
except ImportError:
    pass

try:
    from .operation_dispatcher_mcp import (
        DispatcherMCPTools,
        DispatcherMCPResources,
        DispatcherMCPPrompts,
        OperationDispatcherMCPContext,
        OperationDispatcherMCPServer,
        OperationDispatcherMCPUtility,
    )
except ImportError:
    pass

_OPTIONAL_MCP_EXPORTS = {
    "DispatcherMCPTools",
    "DispatcherMCPResources",
    "DispatcherMCPPrompts",
    "OperationDispatcherMCPContext",
    "OperationDispatcherMCPServer",
    "OperationDispatcherMCPUtility",
    "create_operation_dispatcher_mcp_utility",
    "create_operation_dispatcher_mcp_server",
}

_OPTIONAL_OPENAPI_EXPORTS = {
    "OperationDispatcherOpenAPI",
}

__all__ = [
    "DependencyType",
    "Operation",
    "OperationDependency",
    "History",
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
    "OperationDispatcherReference",
    "OperationDispatcherRuntimeController",
    "DispatcherStateStore",
    "DispatcherRuntimeService",
    "OperationLifecycleService",
    "DispatcherHistoryService",
    "SimulatedOperationRunner",
    "BrowserEventVisualizer",
    "build_history_gantt_figure",
    "show_history_gantt",
    "HistoryAnalyzer",
]

if "OperationDispatcherOpenAPI" in globals():
    __all__.append("OperationDispatcherOpenAPI")

if "OperationDispatcherMCPServer" in globals():
    __all__.extend(
        [
            "DispatcherMCPTools",
            "DispatcherMCPResources",
            "DispatcherMCPPrompts",
            "OperationDispatcherMCPContext",
            "OperationDispatcherMCPServer",
            "OperationDispatcherMCPUtility",
            "create_operation_dispatcher_mcp_utility",
            "create_operation_dispatcher_mcp_server",
        ]
    )


def __getattr__(name: str):
    if name in _OPTIONAL_MCP_EXPORTS:
        raise ImportError(
            "MCP support is optional. Install with `operation-dispatcher[mcp]` "
            "(standalone `fastmcp`) to use MCP exports."
        )
    if name in _OPTIONAL_OPENAPI_EXPORTS:
        raise ImportError(
            "OpenAPI support requires API dependencies. Install with "
            "`operation-dispatcher[api]` to use OpenAPI exports."
        )
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
