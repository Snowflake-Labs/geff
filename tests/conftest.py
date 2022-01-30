import pytest


@pytest.fixture
def pick_mock_data():
    return {"a": {"b": {"c": "dummy"}}}
