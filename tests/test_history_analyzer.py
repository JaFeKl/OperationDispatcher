from operation_dispatcher.diagnostics.history_analyzer import HistoryAnalyzer
from operation_dispatcher.models import ExecutionOutcome, ExecutionState


def test_realistic_history_fixture_has_mixed_operational_outcomes(
    example_history,
) -> None:
    states = {operation.state for operation in example_history.operations}
    outcomes = {operation.outcome for operation in example_history.operations}

    assert states == {
        ExecutionState.COMPLETED,
    }
    assert outcomes == {
        ExecutionOutcome.SUCCESS,
        ExecutionOutcome.CANCELLED,
    }
    assert example_history.operations[1].outcome == ExecutionOutcome.CANCELLED
    assert len(example_history.operations) == 4


def test_realistic_history_fixture_can_drive_kpi_counts(example_history) -> None:
    analyzer = HistoryAnalyzer(example_history)

    assert analyzer._number_of_operations() == 4
    assert analyzer._number_of_completed_operations() == 4
    assert analyzer._number_of_successful_operations() == 3
