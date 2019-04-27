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


@pytest.fixture
def mock_conn_read_data_mixin():
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
@fast_forward_time(0.1, 500)
@timeout(250)
async def test_merger_successful_connection(event_loop, mock_connections,
                                            mock_conn_read_data_mixin):
    conn = mock_connections()
    mock_conn_read_data_mixin(conn, example_replay.data, 0.25, 200)

    merger = Merger.build(merger_config(merger_dict),
                          delay_config(delay_dict))
    await merger.handle_connection(conn)
    await verify_merger_ending_with_data(merger, example_replay.data)


@pytest.mark.asyncio
@fast_forward_time(0.1, 500)
@timeout(250)
async def test_merger_incomplete_header(event_loop, mock_connections,
                                        mock_conn_read_data_mixin):
    conn = mock_connections()
    replay_data = example_replay.header_data[:-100]
    mock_conn_read_data_mixin(conn, replay_data, 0.25, 200)

    merger = Merger.build(merger_config(merger_dict),
                          delay_config(delay_dict))
    with pytest.raises(MalformedDataError):
        await merger.handle_connection(conn)
    await verify_merger_ending_with_data(merger, None)


@pytest.mark.asyncio
@fast_forward_time(0.1, 500)
@timeout(250)
async def test_merger_incomplete_header_then_data(event_loop,
                                                  mock_connections,
                                                  mock_conn_read_data_mixin):
    conn_1 = mock_connections()
    conn_2 = mock_connections()
    replay_data = example_replay.header_data[:-100]
    mock_conn_read_data_mixin(conn_1, replay_data, 0.25, 200)
    mock_conn_read_data_mixin(conn_2, example_replay.data, 0.25, 200)

    merger = Merger.build(merger_config(merger_dict),
                          delay_config(delay_dict))
    with pytest.raises(MalformedDataError):
        await merger.handle_connection(conn_1)
    await merger.handle_connection(conn_2)

    await verify_merger_ending_with_data(merger, example_replay.data)


@pytest.mark.asyncio
@fast_forward_time(0.1, 500)
@timeout(250)
async def test_merger_two_connections(event_loop, mock_connections,
                                      mock_conn_read_data_mixin):
    conn_1 = mock_connections()
    conn_2 = mock_connections()
    replay_data = example_replay.data

    # First has incomplete data, but sends faster
    mock_conn_read_data_mixin(conn_1, replay_data[:-100], 0.4, 160)
    mock_conn_read_data_mixin(conn_2, replay_data, 0.6, 160)

    merger = Merger.build(merger_config(merger_dict),
                          delay_config(delay_dict))
    f_1 = asyncio.ensure_future(merger.handle_connection(conn_1))
    f_2 = asyncio.ensure_future(merger.handle_connection(conn_2))
    await f_1
    await f_2
    await verify_merger_ending_with_data(merger, example_replay.data)


@pytest.mark.asyncio
@fast_forward_time(0.1, 1000)
@timeout(500)
async def test_merger_sequential_connections(event_loop, mock_connections,
                                             mock_conn_read_data_mixin):
    conn_1 = mock_connections()
    conn_2 = mock_connections()
    replay_data = example_replay.data

    # First has incomplete data, but sends faster
    mock_conn_read_data_mixin(conn_1, replay_data[:-100], 0.4, 160)
    mock_conn_read_data_mixin(conn_2, replay_data, 0.6, 160)

    merger = Merger.build(merger_config(merger_dict),
                          delay_config(delay_dict))
    await merger.handle_connection(conn_1)
    await asyncio.sleep(15)
    await merger.handle_connection(conn_2)
    await verify_merger_ending_with_data(merger, example_replay.data)


@pytest.mark.asyncio
@fast_forward_time(0.1, 1000)
@timeout(500)
async def test_merger_refuses_conns(event_loop, mock_connections,
                                    mock_conn_read_data_mixin):
    conn_1 = mock_connections()
    conn_2 = mock_connections()
    replay_data = example_replay.data

    mock_conn_read_data_mixin(conn_1, replay_data, 0.6, 160)
    mock_conn_read_data_mixin(conn_2, replay_data, 0.6, 160)

    merger = Merger.build(merger_config(merger_dict),
                          delay_config(delay_dict))
    await merger.handle_connection(conn_1)
    merger.stop_accepting_connections()
    with pytest.raises(CannotAcceptConnectionError):
        await merger.handle_connection(conn_2)
    await verify_merger_ending_with_data(merger, example_replay.data)


@pytest.mark.asyncio
@fast_forward_time(0.1, 1000)
@timeout(500)
async def test_merger_closes_fast(event_loop, mock_connections,
                                  mock_conn_read_data_mixin):
    conn = mock_connections()
    replay_data = example_replay.data
    mock_conn_read_data_mixin(conn, replay_data, 0.6, 160)

    merger = Merger.build(merger_config(merger_dict),
                          delay_config(delay_dict))
    f = asyncio.ensure_future(merger.handle_connection(conn))
    await asyncio.sleep(45)
    conn.read.side_effect = lambda _: b""   # Simulate connection.close()
    merger.stop_accepting_connections()
    await asyncio.wait_for(merger.wait_for_ended(), 1)
    await asyncio.wait_for(f, 1)
