import pytest
import asyncio
from tests.replays import example_replay
from tests import fast_forward_time, timeout

from replayserver.receive.mergestrategy import MergeStrategies
from replayserver.receive.merger import Merger
from replayserver.server.connection import Connection
from replayserver.errors import MalformedDataError


config = {
    "config_merger_grace_period_time": 30,
    "config_replay_merge_strategy": MergeStrategies.GREEDY,
}


@pytest.fixture
def data_send_mixin():
    def build(mock_connection, replay_data, sleep_time, data_at_once):
        replay_data_pos = 0

        async def read_data(amount):
            nonlocal replay_data_pos
            new_pos = replay_data_pos + data_at_once
            data = replay_data[replay_data_pos:new_pos]
            replay_data_pos = new_pos
            await asyncio.sleep(sleep_time)
            return data

        mock_connection.read.side_effect = read_data
    return build


def test_merger_init():
    Merger.build(**config)


@pytest.mark.asyncio
@fast_forward_time(0.1, 3000)
@timeout(2500)
async def test_merger_successful_connection(event_loop, mock_connections,
                                            data_send_mixin):
    conn = mock_connections(Connection.Type.WRITER, 1)
    replay_data = example_replay.data
    data_send_mixin(conn, replay_data, 0.25, 100)

    merger = Merger.build(**config)
    await merger.handle_connection(conn)
    await merger.wait_for_ended()
    stream = merger.canonical_stream
    assert stream.ended()
    assert stream.header.data + stream.data.bytes() == replay_data


@pytest.mark.asyncio
@fast_forward_time(0.1, 3000)
@timeout(2500)
async def test_merger_incomplete_header(event_loop, mock_connections,
                                        data_send_mixin):
    conn = mock_connections(Connection.Type.WRITER, 1)
    replay_data = example_replay.data[:example_replay.header_size - 100]
    data_send_mixin(conn, replay_data, 0.25, 100)

    merger = Merger.build(**config)
    with pytest.raises(MalformedDataError):
        await merger.handle_connection(conn)
    await merger.wait_for_ended()
    stream = merger.canonical_stream
    assert stream.ended()
    assert stream.header is None
    assert stream.data.bytes() == b""
