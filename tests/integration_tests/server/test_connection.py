import pytest
import asyncio
from asyncio.locks import Event
from asynctest.helpers import exhaust_callbacks
from tests import timeout

from replayserver.server.connectionproducer import ConnectionProducer


@pytest.mark.asyncio
@timeout(0.1)
async def test_producer_sanity_check(unused_tcp_port):
    check_ran = Event()

    async def check(connection):
        data_in = await connection.readexactly(3)
        assert data_in == b"foo"
        is_open = await connection.write(b"bar")
        assert is_open
        connection.close()
        await connection.wait_closed()
        check_ran.set()

    prod = ConnectionProducer(check, unused_tcp_port)
    await prod.start()

    r, w = await asyncio.open_connection('127.0.0.1', unused_tcp_port)
    w.write(b"foo")
    await w.drain()
    data = await r.read()
    assert data == b"bar"
    w.close()

    await check_ran.wait()
    await prod.stop()