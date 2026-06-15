import pytest

from operation_dispatcher import History
from operation_dispatcher.utils.example_history import build_example_history


@pytest.fixture
def example_history() -> History:
    return build_example_history()


def build_realistic_history(resource_id: str = "resource-a") -> History:
    return build_example_history(resource_id=resource_id)
