import pytest
from tests import timeout

from replayserver.receive.merger import Merger


@pytest.fixture
def mock_merge_strategy(mocker):
    class S:
        def stream_added():
            pass

        def stream_removed():
            pass

        def new_data():
            pass

        def finalize():
            pass

        def stream_in_strategy():
            pass

    return mocker.Mock(spec=S)


@pytest.mark.asyncio
@timeout(0.1)
async def test_merger_times_out_after_creation(
        mock_outside_source_stream, mock_merge_strategy):
    merger = Merger(0.01, mock_merge_strategy, mock_outside_source_stream)
    await merger.wait_for_ended()
    mock_outside_source_stream.finish.assert_called()
