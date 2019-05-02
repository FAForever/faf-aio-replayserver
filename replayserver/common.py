import asyncio
from asyncio.locks import Event
from replayserver.collections import AsyncCounter
from replayserver.errors import CannotAcceptConnectionError


class ServesConnections:
    """
    Abstract class, represents serving connections. Handles connections until
    told to stop, then refuses to handle more. After that, once all connections
    are over, performs some housekeeping then reports as finished.
    """

    def __init__(self):
        self._denies_connections = Event()
        self._connection_count = AsyncCounter()
        self._ended = Event()
        asyncio.ensure_future(self._lifetime())

    def _accepts_connections(self):
        return not self._denies_connections.is_set()

    def stop_accepting_connections(self):
        self._denies_connections.set()

    async def _wait_until_all_connections_end(self):
        await self._denies_connections.wait()
        await self._connection_count.wait_until_empty()

    async def handle_connection(self, connection):
        if not self._accepts_connections():
            raise CannotAcceptConnectionError(
                f"{self} no longer accepts connections")
        try:
            self._connection_count.inc()
            return await self._handle_connection(connection)
        finally:
            self._connection_count.dec()

    async def _lifetime(self):
        await self._wait_until_all_connections_end()
        await self._after_connections_end()
        self._ended.set()

    async def wait_for_ended(self):
        await self._ended.wait()

    async def _handle_connection(self, connection):
        raise NotImplementedError

    async def _after_connections_end(self):
        raise NotImplementedError
