import pytest
import asyncio
from tests.replays import example_replay
from tests import fast_forward_time, timeout

from replayserver.receive.mergestrategy import MergeStrategies
from replayserver.receive.merger import Merger
from replayserver.server.connection import Connection


config = {
    "config_merger_grace_period_time": 30,
    "config_replay_merge_strategy": MergeStrategies.GREEDY,
}


def test_merger_init():
    Merger.build(**config)


@pytest.mark.asyncio
@fast_forward_time(0.1, 3000)
@timeout(2500)
async def test_merger_successful_connection(event_loop, mock_connections):
    conn = mock_connections(Connection.Type.WRITER, 1)
    replay_data = example_replay.data
    replay_data_pos = 0

    async def read_data(amount):
        nonlocal replay_data_pos
        new_pos = replay_data_pos + 100
        data = replay_data[replay_data_pos:new_pos]
        replay_data_pos = new_pos
        await asyncio.sleep(0.25)
        return data

    conn.read.side_effect = read_data

    merger = Merger.build(**config)
    await merger.handle_connection(conn)
    stream = merger.canonical_stream
    assert stream.header.data + stream.data.bytes() == replay_data
