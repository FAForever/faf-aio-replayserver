import asyncio
import pytest
from tests import config_from_dict, timeout, fast_forward_time
from tests.replays import diverging_1

from replayserver.server.replay import Replay, ReplayConfig
from replayserver.server.connection import ConnectionHeader

# Suitable for short tests.
config_dict = {
    "forced_end_time": 1000,
    "grace_period": 5,
    "delay": {
        "replay_delay": 5,
        "update_interval": 0.1,
    },
    "merge": {
        "desired_quorum": 2
    }
}


def replay_config(d):
    return ReplayConfig(config_from_dict(d))


def test_replay_init(mock_bookkeeper):
    Replay.build(1, mock_bookkeeper, replay_config(config_dict))


async def do_write(conn, r, chunk):
    for pos in range(0, len(r.data), chunk):
        conn._feed_data(r.data[pos:pos+chunk])
        await asyncio.sleep(0.1)
    conn._feed_eof()
    conn.close()


@pytest.mark.asyncio
@fast_forward_time(1, 120)
@timeout(100)
async def test_replay_diverging_replay(event_loop, mock_bookkeeper,
                                       controlled_connections):

    r = Replay.build(1, mock_bookkeeper, replay_config(config_dict))
    conns = [controlled_connections() for _ in range(0, 6)]
    conn_work = [do_write(c, div, 4000) for c, div in zip(conns, diverging_1)]
    read_conn = controlled_connections()

    head = ConnectionHeader(ConnectionHeader.Type.WRITER, 1, "foo")
    read_head = ConnectionHeader(ConnectionHeader.Type.READER, 1, "foo")
    r_work = [r.handle_connection(head, c) for c in conns]
    w_work = [r.handle_connection(read_head, read_conn)]

    # Version 5 is the best for this replay.
    # The offline-merged replay should be replay 5.
    def check_saved(_, s):
        assert s.header.data + s.data.bytes() == diverging_1[5].data

    mock_bookkeeper.save_replay.side_effect = check_saved

    await asyncio.gather(*(conn_work + r_work + w_work))
    await r.wait_for_ended()
    # Our current strategy should find replay 5.
    assert read_conn._get_mock_write_data() == diverging_1[5].data
