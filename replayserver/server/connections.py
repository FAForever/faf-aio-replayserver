import asyncio

from replayserver import metrics
from replayserver.collections import AsyncSet
from replayserver.errors import BadConnectionError, EmptyConnectionError
from replayserver.server.connection import ConnectionHeader
from replayserver.logging import logger, short_exc


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
        conn_gauge = metrics.ConnectionGauge()
        conn_gauge.set_initial()
        self._connections.add(connection)
        try:
            header = await self._handle_initial_data(connection)
            conn_gauge.set_active(header.type)
            await self._replays.handle_connection(header, connection)
            metrics.successful_conns.inc()
        # Ignore empty connections, these happen often and are not errors
        except EmptyConnectionError as e:
            pass
        except BadConnectionError as e:
            if not connection.closed_by_us():
                logger.info((f"Connection was dropped: {connection}\n"
                             f"Reason: {short_exc(e)}"))
                metrics.failed_conns(e).inc()
            else:
                # Error from connections we force-closed don't matter
                metrics.successful_conns.inc()
        finally:
            await self._cleanup_connection(connection)
            conn_gauge.clear()

    async def _handle_initial_data(self, connection):
        header = await self._header_read(connection)
        connection.add_header(header)
        logger.debug(f"Accepted new connection: {connection}")
        return header

    async def _cleanup_connection(self, connection):
        connection.close()
        await connection.wait_closed()
        logger.debug(f"Finished serving connection: {connection}")
        self._connections.remove(connection)

    async def close_all(self):
        logger.info("Closing all connections")
        for c in self._connections:
            c.close()
        if self._connections:
            await asyncio.wait(
                [connection.wait_closed() for connection in self._connections])

    async def wait_until_empty(self):
        await self._connections.wait_until_empty()
