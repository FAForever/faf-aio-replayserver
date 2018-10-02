import pytest
import asynctest


@pytest.fixture
def mock_bookkeeper():
    class C:
        async def save_replay():
            pass

    return asynctest.Mock(spec=C)


@pytest.fixture
def mock_connection_headers():
    def build(type_, game_id):
        return asynctest.Mock(type=type_, game_id=game_id, name="")

    return build
