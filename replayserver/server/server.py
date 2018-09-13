import asyncio
from replayserver.errors import StreamEndedError
from replayserver.server.connection import Connection
from replayserver.server.replays import Replays


class Server:
    def __init__(self, server, replays, replay_connection_builder):
        self._server = server
        self._replays = replays
        self._replay_connection_builder = replay_connection_builder
        self._connections = set()

    @classmethod
    async def build(cls, port):
        def server(cb):
            return asyncio.streams.start_server(cb, port=port)
        replays = Replays.build()
        return cls(server, replays, Connection)

    async def start(self):
        # A tiny hack - we can't pass in a server directly since we have to
        # register our method as a callback at creation
        self._server = await self._server(self.handle_connection)

    async def stop(self):
        self._server.close()
        await self._server.wait_closed()
        for connection in self._connections:
            connection.close()
        await self._replays.stop()

    async def handle_connection(self, reader, writer):
        connection = self._replay_connection_builder(reader, writer)
        self._connections.add(connection)
        try:
            try:
                await connection.read_header()
            except ValueError as e:
                raise ConnectionError from e
            await self._replays.handle_connection(connection)
        except (ConnectionError, StreamEndedError):
            pass    # TODO - log
        finally:
            connection.close()
            self._connections.remove(connection)
