import pytest


@pytest.fixture
def mock_outside_source_stream(mock_replay_streams):
    class OS:
        def set_header():
            pass

        def feed_data():
            pass

        def finish():
            pass

    mock_replay_stream = mock_replay_streams()
    mock_replay_stream.mock_add_spec(OS)
    return mock_replay_stream
