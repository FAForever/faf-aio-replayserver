import pytest
import asyncio
from tests import timeout

from replayserver.server.connectionproducer import ConnectionProducer


@pytest.mark.asyncio
@timeout(1)
async def test_connectionproducer_connections_work():
    handle_done = asyncio.locks.Event()

    async def handle_conn(conn):
        d = await conn.readexactly(3)
        assert d == b"bar"
        await conn.write(b"foo")
        handle_done.set()

    c = ConnectionProducer(handle_conn, 6660, 0.1)
    await c.start()

    r, w = await asyncio.open_connection('127.0.0.1', 6660)
    w.write(b"bar")
    await w.drain()
    d = await r.readexactly(3)
    assert d == b"foo"
    await handle_done.wait()


@pytest.mark.asyncio
@timeout(1)
async def test_connectionproducer_connections_wont_accept_after_closing():
    handle_done = asyncio.locks.Event()

    async def handle_conn(conn):
        handle_done.set()

    c = ConnectionProducer(handle_conn, 6663, 0.1)
    await c.start()

    r, w = await asyncio.open_connection('127.0.0.1', 6663)
    await handle_done.wait()
    await c.stop()
    with pytest.raises(ConnectionRefusedError):
        await asyncio.open_connection('127.0.0.1', 6663)


@pytest.mark.asyncio
@timeout(1)
async def test_connection_closes_immediately_no_data():
    handle_done = asyncio.locks.Event()
    handled_conn = None

    async def handle_conn(conn):
        nonlocal handled_conn
        handled_conn = conn
        await conn.read(10)
        handle_done.set()

    c = ConnectionProducer(handle_conn, 6661, 0.1)
    await c.start()

    r, w = await asyncio.open_connection('127.0.0.1', 6661)
    await asyncio.sleep(0.2)
    handled_conn.close()
    await handled_conn.wait_closed()
    await asyncio.sleep(0.2)
    assert handle_done.is_set()


@pytest.mark.asyncio
@timeout(1)
async def test_connection_closes_does_not_allow_more_data():
    start_handling = asyncio.locks.Event()
    handle_done = asyncio.locks.Event()
    handled_conn = None

    async def handle_conn(conn):
        nonlocal handled_conn
        handled_conn = conn
        data = b""
        await start_handling.wait()
        while True:
            d = await conn.read(1)
            data += d
            if not d:
                break
            await asyncio.sleep(0.01)
        assert conn.reader.at_eof()
        assert data == b"foo" * 5
        handle_done.set()

    c = ConnectionProducer(handle_conn, 6662, 0.1)
    await c.start()
    r, w = await asyncio.open_connection('127.0.0.1', 6662)

    w.write(b"foo" * 5)
    await w.drain()
    start_handling.set()
    await asyncio.sleep(0.1)
    handled_conn.close()
    await handled_conn.wait_closed()
    w.write(b"foo" * 1000)
    await w.drain()
    await handle_done.wait()
