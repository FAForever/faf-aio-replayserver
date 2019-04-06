import pytest
import asyncio
from tests import config_from_dict, fast_forward_time, timeout
from tests.replays import example_replay

from replayserver.server.replay import Replay, ReplayConfig
from replayserver.server.connection import ConnectionHeader
from replayserver.struct.mangling import END_OF_REPLAY

# Suitable for short tests.
config_dict = {
    "forced_end_time": 1000,
    "grace_period": 5,
    "send": {
        "replay_delay": 0.1,
        "update_interval": 0.1,
    },
    "merge": {
        "stall_check_period": 60
    }
}


def replay_config(d):
    return ReplayConfig(config_from_dict(d))


def test_replay_init(mock_bookkeeper):
    Replay.build(1, mock_bookkeeper, replay_config(config_dict))


@pytest.mark.asyncio
@fast_forward_time(0.1, 20)
@timeout(10)
async def test_replay_doesnt_send_eos_prematurely(
        event_loop, mock_bookkeeper, controlled_connections,
        mock_connection_headers):
    replay = Replay.build(1, mock_bookkeeper, replay_config(config_dict))

    w = controlled_connections()
    w._feed_data(example_replay.header_data)
    w._feed_data(b"a" * 100)
    w._feed_data(END_OF_REPLAY)
    w._feed_eof()
    wh = mock_connection_headers(ConnectionHeader.Type.WRITER, 1)
    r = controlled_connections()
    rh = mock_connection_headers(ConnectionHeader.Type.READER, 1)

    w_coro = asyncio.ensure_future(replay.handle_connection(wh, w))
    r_coro = asyncio.ensure_future(replay.handle_connection(rh, r))
    await w_coro

    await asyncio.sleep(2)  # Not enough to end the whole replay
    expected = example_replay.header_data + b"a" * 100
    assert r._get_mock_write_data() == expected
    await r_coro
    expected += END_OF_REPLAY
    assert r._get_mock_write_data() == expected

    await replay.wait_for_ended()
