from asyncio.locks import Event
from replayserver.collections import AsyncCounter
from contextlib import contextmanager


class CanStopServingConnsMixin:
    """
    Allows to count active connections, stop accepting new ones at some point,
    then wait until all connections are served (therefore no new ones will ever
    arrive).
    """
    def __init__(self):
        self._denies_connections = Event()
        self._connection_count = AsyncCounter()

    def _accepts_connections(self):
        return not self._denies_connections.is_set()

    def stop_accepting_connections(self):
        self._denies_connections.set()

    async def _wait_until_all_connections_end(self):
        await self._denies_connections.wait()
        await self._connection_count.wait_until_empty()

    @contextmanager
    def _count_connection(self):
        self._connection_count.inc()
        try:
            yield
        finally:
            self._connection_count.dec()
