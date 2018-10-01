import asyncio

from replayserver.errors import BadConnectionError
from replayserver.server.connection import Connection, ConnectionHeader
from replayserver.server.replays import Replays


class Server:
    def __init__(self, connection_producer, connections, replays):
        self._replays = replays
        self._connection_producer = connection_producer
        self._connections = connections

    @classmethod
    def build(cls, **kwargs):
        replays = Replays.build(**kwargs)
        conns = Connections.build(**kwargs)
        producer = ConnectionProducer.build(conns.handle_connection, **kwargs)
        return cls(producer, replays, conns)

    async def start(self):
        await self._replays.start()
        await self._connection_producer.start(
            self.connections.handle_connection)

    async def stop(self):
        await self._connection_producer.stop()
        self._connections.close_all()
        await self._replays.stop_all()


class Connections:
    def __init__(self, header_read, replays):
        self._replays = replays
        self._header_read = header_read
        self._connections = set()

    @classmethod
    def build(cls, replays, **kwargs):
        return cls(replays, ConnectionHeader.read)

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

    def close_all(self):
        for connection in self._connections:
            connection.close()


class ConnectionProducer:
    """
    Tiny facade for an asyncio server. There's really nothing to unit test
    here, as any unit tests boil down to 'the method does what it does'.
    Hence, no DI.
    """
    def __init__(self, callback, server_port):
        self._server = None
        self._server_port = server_port
        self._callback = callback

    @classmethod
    def build(cls, callback, *, config_server_port, **kwargs):
        return cls(callback, config_server_port)

    async def start(self):
        self._server = await asyncio.streams.start_server(
            self._make_connection, port=self._server_port)

    async def _make_connection(self, reader, writer):
        connection = Connection(reader, writer)
        await self._callback(connection)

    async def stop(self):
        self._server.close()
        await self._server.wait_closed()
