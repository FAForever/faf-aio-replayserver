import pytest
import asyncio
from tests import timeout, slow_test, docker_faf_db_config, skip_stress_test
from tests.replays import example_replay

from replayserver import Server
from replayserver.receive.mergestrategy import MergeStrategies


config = {
    "merger_grace_period_time": 1,
    "replay_merge_strategy": MergeStrategies.FOLLOW_STREAM,
    "mergestrategy_stall_check_period": 60,
    "sent_replay_delay": 5,
    "sent_replay_position_update_interval": 0.1,
    "replay_forced_end_time": 60,
    "server_port": 15000,
    "db_host": docker_faf_db_config["host"],
    "db_port": docker_faf_db_config["port"],
    "db_user": docker_faf_db_config["user"],
    "db_password": docker_faf_db_config["password"],
    "db_name":     docker_faf_db_config["db"],
    "replay_store_path": "/tmp/replaceme",
    "prometheus_port": None
}
config = {"config_" + k: v for k, v in config.items()}


def test_server_init():
    Server.build(**config)


async def assert_connection_closed(r, w):
    while True:
        d = await r.read(4096)
        if not d:
            break


@slow_test
@pytest.mark.asyncio
@timeout(3)
async def test_server_single_connection(mock_database, tmpdir):
    conf = dict(config)
    conf["config_server_port"] = 15001
    conf["prometheus_port"] = 16001
    conf["config_replay_store_path"] = str(tmpdir)

    await mock_database.add_mock_game((1, 1, 1), [(1, 1), (2, 2)])
    server = Server.build(dep_database=lambda **kwargs: mock_database,
                          **conf)
    await server.start()
    r, w = await asyncio.open_connection('127.0.0.1', 15001)

    w.write(b"P/1/foo\0")
    w.write(example_replay.data)
    await w.drain()
    w.close()
    rep = await server._replays.wait_for_replay(1)
    await rep.wait_for_ended()

    await assert_connection_closed(r, w)
    rfile = list(tmpdir.visit('1.fafreplay'))
    assert len(rfile) == 1


@slow_test
@pytest.mark.asyncio
@timeout(5)
async def test_server_replay_force_end(mock_database, tmpdir):
    conf = dict(config)
    conf["config_server_port"] = 15002
    conf["config_replay_store_path"] = str(tmpdir)
    conf["config_replay_forced_end_time"] = 1
    conf["config_replay_delay"] = 0.5

    await mock_database.add_mock_game((1, 1, 1), [(1, 1), (2, 2)])
    server = Server.build(dep_database=lambda **kwargs: mock_database,
                          **conf)
    await server.start()
    r, w = await asyncio.open_connection('127.0.0.1', 15002)

    async def write_forever():
        w.write(b"P/1/foo\0")
        w.write(example_replay.header_data)
        while True:
            w.write(b"foo")
            await asyncio.sleep(0.05)

    writing = asyncio.ensure_future(write_forever())
    rep = await server._replays.wait_for_replay(1)
    await rep.wait_for_ended()
    writing.cancel()

    await assert_connection_closed(r, w)
    rfile = list(tmpdir.visit('1.fafreplay'))
    assert len(rfile) == 1


@slow_test
@pytest.mark.asyncio
@timeout(3)
async def test_server_force_close_server(mock_database, tmpdir):
    conf = dict(config)
    conf["config_server_port"] = 15003
    conf["config_replay_store_path"] = str(tmpdir)

    await mock_database.add_mock_game((1, 1, 1), [(1, 1), (2, 2)])
    server = Server.build(dep_database=lambda **kwargs: mock_database,
                          **conf)
    await server.start()
    r, w = await asyncio.open_connection('127.0.0.1', 15003)

    async def write_forever():
        w.write(b"P/1/foo\0")
        w.write(example_replay.header_data)
        while True:
            w.write(b"foo")
            await asyncio.sleep(0.05)

    writing = asyncio.ensure_future(write_forever())
    await asyncio.sleep(0.5)
    await server.stop()
    writing.cancel()
    with pytest.raises(ConnectionRefusedError):
        await asyncio.open_connection('127.0.0.1', 15003)

    await assert_connection_closed(r, w)
    rfile = list(tmpdir.visit('1.fafreplay'))
    assert len(rfile) == 1


@slow_test
@pytest.mark.asyncio
@timeout(5)
async def test_server_reader_is_delayed(mock_database, tmpdir):
    conf = dict(config)
    conf["config_sent_replay_delay"] = 0.5
    conf["config_server_port"] = 15004
    conf["config_replay_store_path"] = str(tmpdir)

    await mock_database.add_mock_game((1, 1, 1), [(1, 1), (2, 2)])
    server = Server.build(dep_database=lambda **kwargs: mock_database,
                          **conf)
    await server.start()
    r, w = await asyncio.open_connection('127.0.0.1', 15004)
    r2, w2 = await asyncio.open_connection('127.0.0.1', 15004)
    read_data = bytearray()
    written_data = bytearray()
    # Use large chunk so buffering doesn't affect read data length
    CHUNK = 4096

    async def write_forever():
        nonlocal written_data
        w.write(b"P/1/foo\0")
        w.write(example_replay.header_data)
        written_data += example_replay.header_data
        while True:
            w.write(b"f" * CHUNK)
            written_data += b"f" * CHUNK
            await asyncio.sleep(0.05)

    async def read_forever():
        nonlocal read_data
        w2.write(b"G/1/foo\0")
        while True:
            b = await r2.read(CHUNK)
            if not b:
                break
            read_data += b

    reading = asyncio.ensure_future(write_forever())
    writing = asyncio.ensure_future(read_forever())

    for i in range(20):
        await asyncio.sleep(0.05)
        assert len(read_data) <= max(len(written_data) - 5 * CHUNK,
                                     len(example_replay.header_data))
        assert len(read_data) >= len(written_data) - 15 * CHUNK

    reading.cancel()
    writing.cancel()
    await server.stop()


@skip_stress_test
@pytest.mark.asyncio
@timeout(10)
async def test_server_stress_test(mock_database, tmpdir):
    conf = dict(config)
    conf["config_server_port"] = 15005
    conf["prometheus_port"] = 16005
    conf["config_replay_store_path"] = str(tmpdir)
    conf["config_sent_replay_delay"] = 0.5

    for i in range(1, 50):
        await mock_database.add_mock_game((i, 1, 1), [(1, 1), (2, 2)])

    server = Server.build(dep_database=lambda **kwargs: mock_database,
                          **conf)
    await server.start()

    async def do_write(r, w, i):
        w.write(f"P/{i}/foo\0".encode())
        for pos in range(0, len(example_replay.data), 4000):
            w.write(example_replay.data[pos:pos+4000])
            await w.drain()
            await asyncio.sleep(0.05)
        w.close()

    async def do_read(r, w, i):
        w.write(f"G/{i}/foo\0".encode())
        while True:
            b = await r.read(4000)
            if not b:
                break

    for i in range(1, 50):
        for j in range(5):
            r, w = await asyncio.open_connection('127.0.0.1', 15005)
            asyncio.ensure_future(do_write(r, w, i))
        for j in range(5):
            r, w = await asyncio.open_connection('127.0.0.1', 15005)
            asyncio.ensure_future(do_read(r, w, i))

    await asyncio.sleep(0.5)
    await server._connections.wait_until_empty()
