import asyncio

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
    def build(cls, replays, *, config_connection_header_read_timeout,
              **kwargs):
        timeout = config_connection_header_read_timeout
        return cls(lambda c: ConnectionHeader.read(c, timeout), replays)

    async def handle_connection(self, connection):
        self._connections.add(connection)
        try:
            header = await self._handle_initial_data(connection)
            await self._pass_control_to_replays(connection, header)
            metrics.successful_conns.inc()
        except BadConnectionError as e:
            logger.info(f"Bad connection was dropped; {e.__class__.__name__}: {str(e)}")
            metrics.failed_conns(e).inc()
        finally:
            self._connections.remove(connection)
            await connection.close()

    async def _handle_initial_data(self, connection):
        with metrics.track(metrics.active_conns_by_header(None)):
            header = await self._header_read(connection)
            connection.add_header(header)
            logger.debug(f"Accepted new connection: {connection}")
            return header

    async def _pass_control_to_replays(self, connection, header):
        with metrics.track(metrics.active_conns_by_header(header)):
            await self._replays.handle_connection(header, connection)

    async def close_all(self):
        logger.info("Closing all connections")
        if self._connections:
            await asyncio.wait(
                [connection.close() for connection in self._connections])

    async def wait_until_empty(self):
        await self._connections.wait_until_empty()
