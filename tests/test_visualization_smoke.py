import pytest

pytest.importorskip("plotly")
pytest.importorskip("numpy")
pytest.importorskip("pandas")

from operation_dispatcher import build_history_gantt_figure


def test_history_gantt_can_be_exported_with_visualization_dependencies(example_history):
    figure = build_history_gantt_figure(example_history, title="Release validation")
    assert len(figure.data) > 0
    assert figure.layout.title.text == "Release validation"
    document = figure.to_html(full_html=True, include_plotlyjs="cdn")
    assert "Plotly.newPlot" in document
