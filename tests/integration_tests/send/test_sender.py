import pytest
import asyncio
from tests.replays import example_replay
from tests import fast_forward_time, timeout

from replayserver.send.sender import Sender
from replayserver.struct.header import ReplayHeader


def test_sender_init(outside_source_stream):
    Sender.build(outside_source_stream)


@pytest.mark.asyncio
@fast_forward_time(0.1, 2000)
@timeout(1000)
async def test_sender_one_connection(event_loop, outside_source_stream,
                                     controlled_connections):
    sender = Sender.build(outside_source_stream)
    conn = controlled_connections()

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
    assert conn._get_mock_write_data() == b"data" + replay_data

    sender.stop_accepting_connections()
    await sender.wait_for_ended()
