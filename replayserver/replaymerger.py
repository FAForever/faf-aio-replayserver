import asyncio
from asyncio.locks import Event
from replayserver.errors import StreamEndedError
from replayserver.replaystream import ReplayStreamReader


class ReplayStreamLifetime:
    GRACE_PERIOD = 30

    def __init__(self):
        self._stream_count = 0
        self.ended = Event()
        self._grace_period = None
        self._grace_period_enabled = True

    def stream_added(self):
        self._stream_count += 1
        self._cancel_grace_period()

    def stream_removed(self):
        self._stream_count -= 1
        if self._stream_count == 0:
            self._start_grace_period()

    def disable_grace_period(self):
        self._grace_period_enabled = False
        if self._grace_period is not None:
            self._cancel_grace_period()
            self._start_grace_period()

    def _cancel_grace_period(self):
        if self._grace_period is not None:
            self._grace_period.cancel()
            self._grace_period = None

    def _start_grace_period(self):
        if self._grace_period is None:
            self._grace_period = asyncio.ensure_future(
                self._no_streams_grace_period())

    async def _no_streams_grace_period(self):
        if self._grace_period_enabled:
            grace_period = self.GRACE_PERIOD
        else:
            grace_period = 0
        await asyncio.sleep(grace_period)
        self.ended.set()


class ReplayMerger:
    def __init__(self, loop):
        self._loop = loop
        self._lifetime = ReplayStreamLifetime()
        self._connections = set()
        self.position = 0
        self.data = bytearray()

    async def handle_connection(self, connection):
        if self._lifetime.ended:
            raise StreamEndedError("Tried to add a writer to an ended stream!")
        stream = ReplayStreamReader(connection, self)
        self._connections.add(connection)
        self._lifetime.stream_added()
        await stream._read_all()
        self._lifetime.stream_removed()
        self._connections.remove(connection)

    def do_not_wait_for_more_connections(self):
        self._lifetime.disable_grace_period()

    def close(self):
        for c in self._connections:
            c.close()

    # TODO - use strategies here
    async def on_new_data(self, stream):
        if len(self.data) >= stream.position:
            return
        self.data += stream.data[len(self.data):]

    def get_data(self, start, end):
        return self.data[start:end]

    async def wait_for_ended(self):
        await self._lifetime.ended.wait()
