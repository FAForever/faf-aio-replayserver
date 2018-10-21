import pytest
import asyncio
from tests.replays import example_replay
from tests import fast_forward_time, timeout

from replayserver.send.sender import Sender
from replayserver.struct.header import ReplayHeader


config = {
    "config_sent_replay_delay": 5 * 60,
    "config_sent_replay_position_update_interval": 1,
}


@pytest.fixture
def data_receive_mixin():
    def build(mock_connection, sleep_time):
        mock_connection.configure_mock(_written_data=b"")

        async def write_data(data):
            mock_connection._written_data += data
            await asyncio.sleep(sleep_time)

        mock_connection.write.side_effect = write_data
    return build


def test_sender_init(outside_source_stream):
    Sender.build(outside_source_stream, **config)


@pytest.mark.asyncio
@fast_forward_time(0.1, 2000)
@timeout(1000)
async def test_sender_one_connection(event_loop, outside_source_stream,
                                     mock_connections, data_receive_mixin):
    sender = Sender.build(outside_source_stream, **config)
    conn = mock_connections()
    data_receive_mixin(conn, 0.1)

    f = asyncio.ensure_future(sender.handle_connection(conn))

    outside_source_stream.set_header(ReplayHeader(b"data", {}))
    replay_data = example_replay.main_data
    pos = 0
    while pos < len(replay_data):
        outside_source_stream.feed_data(replay_data[pos:pos + 100])
        pos += 100
        await asyncio.sleep(0.3)
    outside_source_stream.finish()

    await f
    assert conn._written_data == b"data" + replay_data
