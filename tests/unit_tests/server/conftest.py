import pytest
import asynctest


@pytest.fixture
def mock_connection_headers():
    def build(type_, game_id):
        return asynctest.Mock(type=type_, game_id=game_id, name="")

    return build


@pytest.fixture
def mock_conn_plus_head(mock_connections, mock_connection_headers):
    def build(type_, game_id):
        head = mock_connection_headers(type_, game_id)
        conn = mock_connections()
        return head, conn
    return build
