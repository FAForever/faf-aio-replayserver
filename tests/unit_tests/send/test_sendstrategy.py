import pytest
import asyncio
import asynctest
from asynctest.helpers import exhaust_callbacks
from tests import timeout

from replayserver.send.sendstrategy import SendStrategy
from replayserver.errors import MalformedDataError


@pytest.mark.asyncio
@timeout(0.1)
async def test_sendstrategy_send_doesnt_end_until_stream_ends(
        mock_connections, outside_source_stream, mock_replay_headers,
        event_loop):
    mock_header = mock_replay_headers()
    connection = mock_connections()
    outside_source_stream.feed_data(b"aaaaa")
    outside_source_stream.set_header(mock_header)

    sender = SendStrategy(outside_source_stream)
    h = asyncio.ensure_future(sender.send_to(connection))
    await exhaust_callbacks(event_loop)
    assert not h.done()

    outside_source_stream.finish()
    await h


@pytest.mark.asyncio
@timeout(0.1)
async def test_sendstrategy_no_header(mock_connections, outside_source_stream,
                                      event_loop):
    connection = mock_connections()
    sender = SendStrategy(outside_source_stream)
    f = asyncio.ensure_future(sender.send_to(connection))
    await exhaust_callbacks(event_loop)
    outside_source_stream.finish()
    with pytest.raises(MalformedDataError):
        await f
    connection.write.assert_not_called()


@pytest.mark.asyncio
@timeout(0.1)
async def test_sendstrategy_connection_calls(mock_connections,
                                             outside_source_stream,
                                             mock_replay_headers,
                                             event_loop):
    mock_header = mock_replay_headers()
    connection = mock_connections()
    mock_header.data = b"Header"
    sender = SendStrategy(outside_source_stream)
    outside_source_stream.set_header(mock_header)
    outside_source_stream.feed_data(b"Data")
    outside_source_stream.finish()
    await sender.send_to(connection)
    connection.write.assert_has_awaits([asynctest.call(b"Header"),
                                        asynctest.call(b"Data")])


@pytest.mark.asyncio
@timeout(0.1)
async def test_sendstrategy_empty_data(mock_connections, outside_source_stream,
                                       mock_replay_headers, event_loop):
    mock_header = mock_replay_headers()
    connection = mock_connections()
    mock_header.data = b"Header"
    sender = SendStrategy(outside_source_stream)
    outside_source_stream.set_header(mock_header)
    outside_source_stream.finish()
    await sender.send_to(connection)
    connection.write.assert_has_awaits([asynctest.call(b"Header")])
