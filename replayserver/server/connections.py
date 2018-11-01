from replayserver import metrics

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
            header = await self._handle_initial_data(connection)
            await self._pass_control_to_replays(connection, header)
        except BadConnectionError as e:
            logger.info(f"Bad connection was dropped; {e.__class__}: {str(e)}")
        finally:
            self._connections.remove(connection)
            connection.close()
            metrics.served_conns.inc()

    async def _handle_initial_data(self, connection):
        metric = metrics.active_conns.labels(category="initial")
        with metrics.track(metric):
            header = await self._header_read(connection)
            logger.debug(f"Accepted new connection: {header}")
            return header

    async def _pass_control_to_replays(self, connection, header):
        metric = metrics.active_conns.labels(category=header.type.value)
        with metrics.track(metric):
            await self._replays.handle_connection(header, connection)

    def close_all(self):
        logger.info("Closing all connections")
        for connection in self._connections:
            connection.close()

    async def wait_until_empty(self):
        await self._connections.wait_until_empty()
