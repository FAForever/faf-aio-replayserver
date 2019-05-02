import asyncio

from replayserver.server.connection import Connection
from replayserver.logging import logger


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
    def build(cls, callback, server_port):
        return cls(callback, server_port)

    async def start(self):
        self._server = await asyncio.streams.start_server(
            self._make_connection, port=self._server_port)
        logger.info(f"Started listening on {self._server_port}")

    async def _make_connection(self, reader, writer):
        # By default asyncio streams keep around a fairly large write buffer -
        # something around 64kb. We already perform our own buffering and flow
        # control (via 5 minute replay delay and sending data after regular
        # timestamps), so we don't need it.
        # Bufferbloat like this can actually harm us - even though we sent the
        # game's header, it might take minutes before enough (delayed!) replay
        # data accumulates and everything is sent, and until then the game
        # won't load the map and appear frozen.
        writer.transport.set_write_buffer_limits(0)
        connection = Connection(reader, writer)
        await self._callback(connection)

    async def stop(self):
        self._server.close()
        await self._server.wait_closed()
        logger.info(f"Stopped listening on {self._server_port}")
