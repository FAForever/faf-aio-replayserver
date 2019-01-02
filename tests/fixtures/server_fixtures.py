import os
import asyncio

import pytest

from replay_server.constants import PORT
from replay_server.server import ReplayServer


__all__ = ('server', 'client')


if not int(os.environ.get("LIVE_TEST", 0)):
    @pytest.yield_fixture
    async def server(event_loop, unused_tcp_port, ):
        replay_server = ReplayServer(unused_tcp_port)

        server_ = await asyncio.start_server(
            replay_server.handle_connection,
            port=unused_tcp_port,
            loop=event_loop
        )
        replay_server._server = server_

        yield server_

        # let the replay_server process handle_connection till end
        await asyncio.sleep(.1, event_loop)

        # Server will stop earlier than client.
        # Unfortunately bad fixtures dependency.
        await replay_server.stop()


    @pytest.yield_fixture
    async def client(server, event_loop, unused_tcp_port):
        clients = []

        async def new_client():
            client_ = await asyncio.streams.open_connection(port=unused_tcp_port, loop=event_loop)
            clients.append(client_)
            return client_

        yield new_client

        for reader, writer in clients:
            writer.close()

else:
    @pytest.yield_fixture
    async def client(event_loop):
        clients = []

        async def new_client():
            client_ = await asyncio.streams.open_connection(port=PORT, loop=event_loop)
            clients.append(client_)
            return client_

        yield new_client

        for reader, writer in clients:
            writer.close()
