import pytest
import asyncio
import copy

from everett import ConfigurationError
from tests import timeout, slow_test, docker_faf_db_config, skip_stress_test, \
    config_from_dict
from tests.replays import example_replay
from replayserver import Server
from replayserver.server.server import MainConfig


config_dict = {
    "log_level": "INFO",
    "server": {
        "port": 15000,
        "prometheus_port": "None",
        "connection_header_read_timeout": 6 * 60 * 60
    },
    "db": {
        "host": docker_faf_db_config["host"],
        "port": docker_faf_db_config["port"],
        "user": docker_faf_db_config["user"],
        "password": docker_faf_db_config["password"],
        "name": docker_faf_db_config["db"],
    },
    "storage": {
        "vault_path": "/tmp/replaceme"
    },
    "replay": {
        "forced_end_time": 60,
        "grace_period": 1,
        "delay": {
            "replay_delay": 5,
            "update_interval": 0.1,
        },
        "merge": {
            "desired_quorum": 2
        }
    },
}


def server_config(d):
    return MainConfig(config_from_dict(d))


def test_server_init(tmpdir):
    conf = copy.deepcopy(config_dict)
    conf["storage"]["vault_path"] = str(tmpdir)
    Server.build(config=server_config(conf))


async def assert_connection_closed(r, w):
    while True:
        d = await r.read(4096)
        if not d:
            break


# Some recursive function magic below so we can test the config dict with every
# key changed in some way separately
# It works, I promise
def act_on_each_key(d, actor):
    newd = copy.deepcopy(d)
    for k, v in d.items():
        if not isinstance(v, dict):
            if not actor.will_act(newd, k):
                continue
            actor.act(newd, k)
            yield newd
            newd[k] = v
        else:
            for newv in act_on_each_key(v, actor.recurse(k)):
                newd[k] = newv
                yield newd
            newd[k] = v


def remove_each_key(d):
    class ActorRemove:
        def act(self, d, k):
            del d[k]

        def will_act(self, d, k):
            return True

        def recurse(self, k):
            return self

    return act_on_each_key(d, ActorRemove())


def replace_each_key(d, repld):
    class ActorReplace:
        def __init__(self, repld):
            self._repld = repld

        def will_act(self, d, k):
            return self._repld is not None and k in self._repld

        def act(self, d, k):
            d[k] = self._repld[k]

        def recurse(self, k):
            return ActorReplace(self._repld.get(k))

    return act_on_each_key(d, ActorReplace(repld))


def test_server_good_config(tmpdir):
    good_conf = copy.deepcopy(config_dict)
    good_conf["storage"]["vault_path"] = str(tmpdir)
    MainConfig(config_from_dict(good_conf))
    good_conf["log_level"] = 20
    MainConfig(config_from_dict(good_conf))


def test_server_bad_config(tmpdir):
    good_conf = copy.deepcopy(config_dict)
    good_conf["storage"]["vault_path"] = str(tmpdir)

    for partial_conf in remove_each_key(good_conf):
        with pytest.raises(ConfigurationError):
            MainConfig(config_from_dict(partial_conf))

    bad_conf = {
        "log_level": "foo",
        "server": {
            "port": "-50",
            "prometheus_port": "bar",
            "connection_header_read_timeout": "-50"
        },
        "db": {
            "port": "0",
        },
        "storage": {
            "vault_path": "/certainly_doesnt_exist"
        },
        "replay": {
            "forced_end_time": "0",
            "grace_period": "-1",
            "delay": {
                "replay_delay": "-10",
                "update_interval": "0",
            },
            "merge": {
                "strategy": "DEFINITELY_NONEXISTENT",
                "strategy_config": {
                    "follow_stream": {
                        "stall_check_period": "0"
                    }
                }
            }
        },
    }

    for partially_bad_conf in replace_each_key(good_conf, bad_conf):
        with pytest.raises(ConfigurationError):
            MainConfig(config_from_dict(partially_bad_conf))


@slow_test
@pytest.mark.asyncio
@timeout(3)
async def test_server_single_connection(mock_database, tmpdir,
                                        unused_tcp_port_factory):
    s_port, p_port = [unused_tcp_port_factory() for i in range(2)]
    conf = copy.deepcopy(config_dict)
    conf["server"]["port"] = s_port
    conf["server"]["prometheus_port"] = p_port
    conf["storage"]["vault_path"] = str(tmpdir)

    await mock_database.add_mock_game((1, 1, 1), [(1, 1), (2, 2)])
    server = Server.build(dep_database=lambda _: mock_database,
                          config=server_config(conf))
    await server.start()
    r, w = await asyncio.open_connection('127.0.0.1', s_port)

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
async def test_server_replay_force_end(mock_database, tmpdir, unused_tcp_port):
    s_port = unused_tcp_port
    conf = copy.deepcopy(config_dict)
    conf["server"]["port"] = s_port
    conf["storage"]["vault_path"] = str(tmpdir)
    conf["replay"]["forced_end_time"] = 1
    conf["replay"]["delay"]["replay_delay"] = 0.5

    await mock_database.add_mock_game((1, 1, 1), [(1, 1), (2, 2)])
    server = Server.build(dep_database=lambda _: mock_database,
                          config=server_config(conf))
    await server.start()
    r, w = await asyncio.open_connection('127.0.0.1', s_port)

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
async def test_server_force_close_server(mock_database, tmpdir,
                                         unused_tcp_port):
    s_port = unused_tcp_port
    conf = copy.deepcopy(config_dict)
    conf["server"]["port"] = s_port
    conf["storage"]["vault_path"] = str(tmpdir)

    await mock_database.add_mock_game((1, 1, 1), [(1, 1), (2, 2)])
    server = Server.build(dep_database=lambda _: mock_database,
                          config=server_config(conf))
    await server.start()
    r, w = await asyncio.open_connection('127.0.0.1', s_port)

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
        await asyncio.open_connection('127.0.0.1', s_port)

    await assert_connection_closed(r, w)
    rfile = list(tmpdir.visit('1.fafreplay'))
    assert len(rfile) == 1


@slow_test
@pytest.mark.asyncio
@timeout(5)
async def test_server_reader_is_delayed(mock_database, tmpdir,
                                        unused_tcp_port):
    s_port = unused_tcp_port
    conf = copy.deepcopy(config_dict)
    conf["replay"]["delay"]["replay_delay"] = 0.5
    conf["server"]["port"] = s_port
    conf["storage"]["vault_path"] = str(tmpdir)

    await mock_database.add_mock_game((1, 1, 1), [(1, 1), (2, 2)])
    server = Server.build(dep_database=lambda _: mock_database,
                          config=server_config(conf))
    await server.start()
    r, w = await asyncio.open_connection('127.0.0.1', s_port)
    r2, w2 = await asyncio.open_connection('127.0.0.1', s_port)
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
@timeout(30)
async def test_server_stress_test(mock_database, tmpdir,
                                  unused_tcp_port_factory):
    s_port, p_port = [unused_tcp_port_factory() for i in range(2)]
    conf = copy.deepcopy(config_dict)
    conf["server"]["port"] = s_port
    conf["server"]["prometheus_port"] = p_port
    conf["storage"]["vault_path"] = str(tmpdir)
    conf["replay"]["delay"]["replay_delay"] = 0.5

    for i in range(1, 50):
        await mock_database.add_mock_game((i, 1, 1), [(1, 1), (2, 2)])

    server = Server.build(dep_database=lambda _: mock_database,
                          config=server_config(conf))
    await server.start()

    async def do_write(r, w, i):
        w.write(f"P/{i}/foo\0".encode())
        for pos in range(0, len(example_replay.data), 40):
            w.write(example_replay.data[pos:pos + 40])
            await w.drain()
            await asyncio.sleep(0.01)
        w.close()

    async def do_read(r, w, i):
        w.write(f"G/{i}/foo\0".encode())
        while True:
            b = await r.read(40)
            if not b:
                break

    for i in range(1, 5):
        for _ in range(5):
            r, w = await asyncio.open_connection('127.0.0.1', s_port)
            asyncio.ensure_future(do_write(r, w, i))
        for _ in range(5):
            r, w = await asyncio.open_connection('127.0.0.1', s_port)
            asyncio.ensure_future(do_read(r, w, i))

    await asyncio.sleep(0.5)
    await server._connections.wait_until_empty()
    await server.stop()
