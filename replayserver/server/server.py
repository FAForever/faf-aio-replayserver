import asyncio
from contextlib import contextmanager

from replayserver.errors import BadConnectionError
from replayserver.server.connection import Connection, ConnectionHeader
from replayserver.server.replays import Replays


class Server:
    def __init__(self, server, replays, connection_builder, header_read):
        self._server = server
        self._replays = replays
        self._connection_builder = connection_builder
        self._header_read = header_read
        self._connections = set()

    @classmethod
    def build(cls, *, config_server_port, **kwargs):
        def server(cb):
            return asyncio.streams.start_server(cb, port=config_server_port)
        replays = Replays.build(**kwargs)
        return cls(server, replays, Connection, ConnectionHeader.read)

    async def start(self):
        await self._replays.start()
        # A tiny hack - we can't pass in a server directly since we have to
        # register our method as a callback at creation
        self._server = await self._server(self.handle_connection)

    async def stop(self):
        self._server.close()
        await self._server.wait_closed()
        for connection in self._connections:
            connection.close()
        await self._replays.stop()

    @contextmanager
    def _get_connection(self, reader, writer):
        connection = self._connection_builder(reader, writer)
        self._connections.add(connection)
        try:
            yield connection
        finally:
            self._connections.remove(connection)
            connection.close()

    async def handle_connection(self, reader, writer):
        with self._get_connection(reader, writer) as connection:
            try:
                header = self._header_read(connection)
                replay = await self._replays.get_matching_replay(header)
                await replay._handle_connection(header, connection)
            except BadConnectionError:
                pass    # TODO - log
