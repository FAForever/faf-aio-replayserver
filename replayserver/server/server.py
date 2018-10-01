import asyncio

from replayserver.errors import BadConnectionError
from replayserver.server.connection import Connection, ConnectionHeader
from replayserver.server.replays import Replays


class Server:
    def __init__(self, connection_producer, replays, header_read):
        self._replays = replays
        self._connection_producer = connection_producer
        self._header_read = header_read
        self._connections = set()

    @classmethod
    def build(cls, **kwargs):
        producer = ConnectionProducer.build(**kwargs)
        replays = Replays.build(**kwargs)
        return cls(producer, replays, ConnectionHeader.read)

    async def start(self):
        await self._replays.start()
        await self._connection_producer.start(self.handle_connection)

    async def stop(self):
        await self._connection_producer.stop()
        for connection in self._connections:
            connection.close()
        await self._replays.stop()

    async def handle_connection(self, connection):
        self._connections.add(connection)
        try:
            header = self._header_read(connection)
            replay = await self._replays.get_matching_replay(header)
            await replay._handle_connection(header, connection)
        except BadConnectionError:
            pass    # TODO - log
        finally:
            self._connections.remove(connection)
            connection.close()


class ConnectionProducer:
    """
    Tiny facade for an asyncio server. There's really nothing to unit test
    here, as any unit tests boil down to 'the method does what it does'.
    Hence, no DI.
    """
    def __init__(self, server_port):
        self._server = None
        self._server_port = server_port
        self._callback = None

    @classmethod
    def build(cls, *, config_server_port, **kwargs):
        return cls(config_server_port)

    async def start(self, callback):
        self._callback = callback
        self._server = await asyncio.streams.start_server(
            self._make_connection, port=self._server_port)

    async def _make_connection(self, reader, writer):
        connection = Connection(reader, writer)
        await self._callback(connection)

    async def stop(self):
        self._server.close()
        await self._server.wait_closed()
