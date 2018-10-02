import asyncio

from replayserver.server.connection import Connection


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
