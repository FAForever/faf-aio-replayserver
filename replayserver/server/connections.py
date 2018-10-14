from replayserver.collections import AsyncSet
from replayserver.errors import BadConnectionError
from replayserver.server.connection import ConnectionHeader
from replayserver.logging import logger


class Connections:
    def __init__(self, header_read, replays):
        self._replays = replays
        self._header_read = header_read
        self._connections = AsyncSet()

    @classmethod
    def build(cls, replays, **kwargs):
        return cls(ConnectionHeader.read, replays)

    async def handle_connection(self, connection):
        self._connections.add(connection)
        try:
            header = await self._header_read(connection)
            self._debug_new_connection(header)
            await self._replays.handle_connection(header, connection)
        except BadConnectionError as e:
            logger.info(f"Bad connection was dropped; {e.__class__}: {str(e)}")
        finally:
            self._connections.remove(connection)
            connection.close()

    def _debug_new_connection(self, header):
        logger.debug((f"Accepted new connection: {header.type.value}, "
                      f"id {header.game_id}, name {header.game_name}"))

    def close_all(self):
        logger.info("Closing all connections")
        for connection in self._connections:
            connection.close()

    async def wait_until_empty(self):
        await self._connections.wait_until_empty()
