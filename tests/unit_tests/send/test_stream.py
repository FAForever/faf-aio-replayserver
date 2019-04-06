import pytest
import asyncio
import asynctest
from asynctest.helpers import exhaust_callbacks
from tests import timeout

from replayserver.send.stream import DelayedReplayStream, ReplayStreamWriter


class TestMangler:
    def mangle(self, data):
        return data

    def drain(self):
        return b""


@pytest.fixture
def id_mangler():
    return TestMangler


@pytest.fixture
def mock_mangler(mocker):
    return mocker.Mock(spec=TestMangler)


@pytest.mark.asyncio
@timeout(0.1)
async def test_stream_writer_respects_mangler(
        controlled_connections, outside_source_stream, mock_replay_headers,
        mock_mangler, event_loop):
    connection = controlled_connections()
    mock_header = mock_replay_headers()
    mock_header.data = b"Header"
    outside_source_stream.set_header(mock_header)
    outside_source_stream.feed_data(b"aaaaa")
    outside_source_stream.finish()
    mock_mangler.mangle.side_effect = lambda d: b"".join(b"b" for v in d)
    mock_mangler.drain.return_value = b"c"

    sender = ReplayStreamWriter(outside_source_stream, lambda: mock_mangler)
    await sender.send_to(connection)
    assert connection._get_mock_write_data() == b"Headerbbbbbc"


@pytest.mark.asyncio
@timeout(0.1)
async def test_stream_writer_send_doesnt_end_until_stream_ends(
        mock_connections, outside_source_stream, mock_replay_headers,
        id_mangler, event_loop):
    mock_header = mock_replay_headers()
    connection = mock_connections()
    outside_source_stream.feed_data(b"aaaaa")
    outside_source_stream.set_header(mock_header)

    sender = ReplayStreamWriter(outside_source_stream, id_mangler)
    h = asyncio.ensure_future(sender.send_to(connection))
    await exhaust_callbacks(event_loop)
    assert not h.done()

    outside_source_stream.finish()
    await h


@pytest.mark.asyncio
@timeout(0.1)
async def test_stream_writer_no_header(mock_connections, outside_source_stream,
                                       id_mangler, event_loop):
    connection = mock_connections()
    sender = ReplayStreamWriter(outside_source_stream, id_mangler)
    f = asyncio.ensure_future(sender.send_to(connection))
    await exhaust_callbacks(event_loop)
    outside_source_stream.finish()
    await f     # We expect no errors
    connection.write.assert_not_called()


@pytest.mark.asyncio
@timeout(0.1)
async def test_stream_writer_connection_calls(mock_connections,
                                              outside_source_stream,
                                              mock_replay_headers,
                                              id_mangler,
                                              event_loop):
    mock_header = mock_replay_headers()
    connection = mock_connections()
    mock_header.data = b"Header"
    sender = ReplayStreamWriter(outside_source_stream, id_mangler)
    outside_source_stream.set_header(mock_header)
    outside_source_stream.feed_data(b"Data")
    outside_source_stream.finish()
    await sender.send_to(connection)
    connection.write.assert_has_awaits([asynctest.call(b"Header"),
                                        asynctest.call(b"Data")])


@pytest.mark.asyncio
@timeout(0.1)
async def test_stream_writer_empty_data(mock_connections,
                                        outside_source_stream,
                                        mock_replay_headers,
                                        id_mangler,
                                        event_loop):
    mock_header = mock_replay_headers()
    connection = mock_connections()
    mock_header.data = b"Header"
    sender = ReplayStreamWriter(outside_source_stream, id_mangler)
    outside_source_stream.set_header(mock_header)
    outside_source_stream.finish()
    await sender.send_to(connection)
    connection.write.assert_has_awaits([asynctest.call(b"Header")])


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
