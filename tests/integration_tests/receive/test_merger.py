import pytest
import asyncio

from tests.replays import example_replay
from tests import fast_forward_time, timeout, config_from_dict

from replayserver.receive.merger import Merger, MergerConfig, DelayConfig
from replayserver.errors import MalformedDataError, CannotAcceptConnectionError


merger_dict = {
    "desired_quorum": 2
}

delay_dict = {
    "replay_delay": 1,
    "update_interval": 0.1
}


def merger_config(d):
    return MergerConfig(config_from_dict(d))


def delay_config(d):
    return DelayConfig(config_from_dict(d))


def mock_conn_feed_data(mock_connection, replay_data, sleep_time,
                        data_at_once):
    async def _do_feed():
        replay_data_pos = 0
        while replay_data_pos < len(replay_data):
            new_pos = replay_data_pos + data_at_once
            data = replay_data[replay_data_pos:new_pos]
            replay_data_pos = new_pos
            mock_connection._feed_data(data)
            await asyncio.sleep(sleep_time)
        mock_connection._feed_eof()

    return asyncio.ensure_future(_do_feed())


async def verify_merger_ending_with_data(merger, data):
    merger.stop_accepting_connections()
    await merger.wait_for_ended()
    stream = merger.canonical_stream
    assert stream.ended()
    if data is None:
        assert stream.header is None
        assert stream.data.bytes() == b""
    else:
        assert stream.header.data + stream.data.bytes() == data


def test_merger_init():
    Merger.build(merger_config(merger_dict),
                 delay_config(delay_dict))


@pytest.mark.asyncio
@fast_forward_time(500)
@timeout(250)
async def test_merger_successful_connection(controlled_connections):
    conn = controlled_connections()

    merger = Merger.build(merger_config(merger_dict),
                          delay_config(delay_dict))

    f = mock_conn_feed_data(conn, example_replay.data, 0.25, 200)

    await merger.handle_connection(conn)
    await verify_merger_ending_with_data(merger, example_replay.data)
    await f


@pytest.mark.asyncio
@fast_forward_time(500)
@timeout(250)
async def test_merger_incomplete_header(controlled_connections):
    conn = controlled_connections()
    replay_data = example_replay.header_data[:-100]

    merger = Merger.build(merger_config(merger_dict),
                          delay_config(delay_dict))
    f = mock_conn_feed_data(conn, replay_data, 0.25, 200)
    with pytest.raises(MalformedDataError):
        await merger.handle_connection(conn)
    await verify_merger_ending_with_data(merger, None)
    await f


@pytest.mark.asyncio
@fast_forward_time(500)
@timeout(250)
async def test_merger_incomplete_header_then_data(controlled_connections):
    conn_1 = controlled_connections()
    conn_2 = controlled_connections()
    replay_data = example_replay.header_data[:-100]
    merger = Merger.build(merger_config(merger_dict),
                          delay_config(delay_dict))

    f1 = mock_conn_feed_data(conn_1, replay_data, 0.25, 200)
    f2 = mock_conn_feed_data(conn_2, example_replay.data, 0.25, 200)
    with pytest.raises(MalformedDataError):
        await merger.handle_connection(conn_1)
    await merger.handle_connection(conn_2)

    await verify_merger_ending_with_data(merger, example_replay.data)
    await f1
    await f2


@pytest.mark.asyncio
@fast_forward_time(500)
@timeout(250)
async def test_merger_two_connections(controlled_connections):
    conn_1 = controlled_connections()
    conn_2 = controlled_connections()
    replay_data = example_replay.data
    merger = Merger.build(merger_config(merger_dict),
                          delay_config(delay_dict))

    # First has incomplete data, but sends faster
    f1 = mock_conn_feed_data(conn_1, replay_data[:-100], 0.4, 160)
    f2 = mock_conn_feed_data(conn_2, replay_data, 0.6, 160)
    f_1 = asyncio.ensure_future(merger.handle_connection(conn_1))
    f_2 = asyncio.ensure_future(merger.handle_connection(conn_2))
    await f_1
    await f_2
    await verify_merger_ending_with_data(merger, example_replay.data)
    await f1
    await f2


@pytest.mark.asyncio
@fast_forward_time(1000)
@timeout(500)
async def test_merger_sequential_connections(controlled_connections):
    conn_1 = controlled_connections()
    conn_2 = controlled_connections()
    replay_data = example_replay.data
    merger = Merger.build(merger_config(merger_dict),
                          delay_config(delay_dict))

    # First has incomplete data, but sends faster
    f1 = mock_conn_feed_data(conn_1, replay_data[:-100], 0.4, 160)
    await merger.handle_connection(conn_1)
    await f1
    await asyncio.sleep(15)

    f2 = mock_conn_feed_data(conn_2, replay_data, 0.6, 160)
    await merger.handle_connection(conn_2)
    await verify_merger_ending_with_data(merger, example_replay.data)
    await f2


@pytest.mark.asyncio
@fast_forward_time(1000)
@timeout(500)
async def test_merger_refuses_conns(controlled_connections):
    conn_1 = controlled_connections()
    conn_2 = controlled_connections()
    replay_data = example_replay.data
    merger = Merger.build(merger_config(merger_dict),
                          delay_config(delay_dict))

    f1 = mock_conn_feed_data(conn_1, replay_data, 0.6, 160)
    f2 = mock_conn_feed_data(conn_2, replay_data, 0.6, 160)

    await merger.handle_connection(conn_1)
    merger.stop_accepting_connections()
    with pytest.raises(CannotAcceptConnectionError):
        await merger.handle_connection(conn_2)
    await verify_merger_ending_with_data(merger, example_replay.data)
    await f1
    await f2


@pytest.mark.asyncio
@fast_forward_time(1000)
@timeout(500)
async def test_merger_closes_fast(controlled_connections):
    conn = controlled_connections()
    replay_data = example_replay.data
    merger = Merger.build(merger_config(merger_dict),
                          delay_config(delay_dict))

    feed = mock_conn_feed_data(conn, replay_data, 0.6, 160)

    f = asyncio.ensure_future(merger.handle_connection(conn))
    await asyncio.sleep(10)
    feed.cancel()
    conn._feed_eof()
    merger.stop_accepting_connections()
    await asyncio.wait_for(merger.wait_for_ended(), 3)
    await asyncio.wait_for(f, 3)
