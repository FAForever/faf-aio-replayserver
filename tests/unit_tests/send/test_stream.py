import pytest
import asyncio
import asynctest
from asynctest.helpers import exhaust_callbacks
from tests import timeout

from replayserver.send.stream import DelayedReplayStream


@pytest.fixture
def mock_timestamp(locked_mock_coroutines):
    end, wait = locked_mock_coroutines()
    stamp_list = []

    async def mock_stamps():
        while stamp_list:
            yield stamp_list.pop(0)
            await wait()
            if not stamp_list:
                return

    mock_stamp = asynctest.Mock(spec=["timestamps"], _stamps=stamp_list,
                                _resume_stamps=end)
    mock_stamp.timestamps.side_effect = mock_stamps
    return mock_stamp


@pytest.mark.asyncio
@timeout(1)
async def test_delayed_stream_header(outside_source_stream, mock_timestamp,
                                     event_loop):
    stream = DelayedReplayStream(outside_source_stream, mock_timestamp)

    f = asyncio.ensure_future(stream.wait_for_header())
    await exhaust_callbacks(event_loop)
    assert not f.done()

    outside_source_stream.set_header("Header")
    await exhaust_callbacks(event_loop)
    assert f.done()
    h = await f
    assert h == "Header"
