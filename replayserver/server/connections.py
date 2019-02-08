import asyncio

from replayserver import metrics
from replayserver.collections import AsyncSet
from replayserver.errors import BadConnectionError, EmptyConnectionError
from replayserver.server.connection import ConnectionHeader
from replayserver.logging import logger


class Connections:
    def __init__(self, header_read, replays):
        self._replays = replays
        self._header_read = header_read
        self._connections = AsyncSet()

    @classmethod
    def build(cls, replays, header_read_timeout):
        return cls(lambda c: ConnectionHeader.read(c, header_read_timeout),
                   replays)

    async def handle_connection(self, connection):
        self._connections.add(connection)
        try:
            header = await self._handle_initial_data(connection)
            await self._pass_control_to_replays(connection, header)
            metrics.successful_conns.inc()
        except BadConnectionError as e:
            # Ignore empty connections, these happen often and are not errors
            if isinstance(e, EmptyConnectionError):
                return
            logger.info(f"Bad connection was dropped; {e.__class__.__name__}: {str(e)}")
            metrics.failed_conns(e).inc()
        finally:
            connection.close()
            await connection.wait_closed()
            self._connections.remove(connection)

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
        for c in self._connections:
            c.close()
        if self._connections:
            await asyncio.wait(
                [connection.wait_closed() for connection in self._connections])

    async def wait_until_empty(self):
        await self._connections.wait_until_empty()
