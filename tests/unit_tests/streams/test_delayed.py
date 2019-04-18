import pytest
import asyncio
import asynctest
from asynctest.helpers import exhaust_callbacks
from tests import timeout

from replayserver.streams import DelayedReplayStream


@pytest.fixture
def mock_timestamp(blockable_coroutines):
    wait = blockable_coroutines()
    stamp_list = []

    async def mock_stamps():
        while True:
            if stamp_list:
                yield stamp_list.pop(0)
            else:
                await wait()
                if not stamp_list:
                    return

    def next_stamp(pos):
        stamp_list.append(pos)
        wait._lock.set()
        wait._lock.clear()

    def end_stamps():
        wait._lock.set()

    mock_stamp = asynctest.Mock(spec=["timestamps"], _stamps=stamp_list,
                                _resume_stamps=wait, _next_stamp=next_stamp,
                                _end_stamps=end_stamps)
    mock_stamp.timestamps.side_effect = mock_stamps
    return mock_stamp


@pytest.mark.asyncio
@timeout(0.1)
async def test_delayed_stream_header(outside_source_stream, mock_timestamp,
                                     event_loop):
    stream = DelayedReplayStream(outside_source_stream, mock_timestamp)

    f = asyncio.ensure_future(stream.wait_for_header())
    await exhaust_callbacks(event_loop)
    assert not f.done()

    outside_source_stream.set_header("Header")
    assert stream.header == "Header"
    await exhaust_callbacks(event_loop)
    assert f.done()
    h = await f
    assert h == "Header"

    mock_timestamp._end_stamps()
    await exhaust_callbacks(event_loop)


@pytest.mark.asyncio
@timeout(0.1)
async def test_stream_ends_before_header(outside_source_stream, mock_timestamp,
                                         event_loop):
    stream = DelayedReplayStream(outside_source_stream, mock_timestamp)
    outside_source_stream.finish()
    mock_timestamp._end_stamps()
    assert (await stream.wait_for_header()) is None


@pytest.mark.asyncio
@timeout(0.1)
async def test_delayed_stream_data(outside_source_stream, mock_timestamp,
                                   event_loop):
    stream = DelayedReplayStream(outside_source_stream, mock_timestamp)

    outside_source_stream.set_header("Header")
    outside_source_stream.feed_data(b"abcde")
    await exhaust_callbacks(event_loop)
    assert len(stream.data) == 0

    mock_timestamp._next_stamp(0)
    await exhaust_callbacks(event_loop)
    assert len(stream.data) == 0

    mock_timestamp._next_stamp(0)
    await exhaust_callbacks(event_loop)
    assert len(stream.data) == 0

    mock_timestamp._next_stamp(3)
    await exhaust_callbacks(event_loop)
    assert len(stream.data) == 3
    assert stream.data.bytes() == b"abc"

    mock_timestamp._next_stamp(4)
    await exhaust_callbacks(event_loop)
    assert len(stream.data) == 4
    assert stream.data.bytes() == b"abcd"

    outside_source_stream.feed_data(b"fg")
    await exhaust_callbacks(event_loop)
    assert len(stream.data) == 4
    assert stream.data.bytes() == b"abcd"

    mock_timestamp._next_stamp(7)
    await exhaust_callbacks(event_loop)
    assert len(stream.data) == 7
    assert stream.data.bytes() == b"abcdefg"

    mock_timestamp._end_stamps()
    await exhaust_callbacks(event_loop)


@pytest.mark.asyncio
@timeout(0.1)
async def test_stamps_ending_end_stream(outside_source_stream, mock_timestamp,
                                        event_loop):
    stream = DelayedReplayStream(outside_source_stream, mock_timestamp)

    outside_source_stream.set_header("Header")
    outside_source_stream.feed_data(b"abcde")
    mock_timestamp._next_stamp(5)
    await exhaust_callbacks(event_loop)
    assert len(stream.data) == 5

    mock_timestamp._end_stamps()
    await exhaust_callbacks(event_loop)
    assert stream.ended()
    d = await stream.wait_for_data(5)
    assert d == b""

    mock_timestamp._end_stamps()
    await exhaust_callbacks(event_loop)


@pytest.mark.asyncio
@timeout(0.1)
async def test_delayed_stream_data_methods(outside_source_stream,
                                           mock_timestamp,
                                           event_loop):
    stream = DelayedReplayStream(outside_source_stream, mock_timestamp)

    outside_source_stream.set_header("Header")
    outside_source_stream.feed_data(b"abcde")
    mock_timestamp._next_stamp(3)
    await exhaust_callbacks(event_loop)

    assert len(stream.data) == 3
    assert stream.data[1:] == b"bc"
    assert stream.data[1:4] == b"bc"
    assert stream.data[1:2] == b"b"
    assert stream.data.bytes() == b"abc"

    mock_timestamp._end_stamps()
    await exhaust_callbacks(event_loop)
