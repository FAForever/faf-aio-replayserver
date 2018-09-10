import asyncio
from asyncio.locks import Event
from replayserver.errors import StreamEndedError
from replayserver.replaystream import ConnectionReplayStream
from replayserver.mergestrategy import GreedyMergeStrategy


class ReplayStreamLifetime:
    GRACE_PERIOD = 30

    def __init__(self):
        self._stream_count = 0
        self.ended = Event()
        self._grace_period = None
        self._grace_period_enabled = True

    def stream_added(self):
        if self.ended.is_set():
            raise ValueError("Tried to add a writer to an ended stream!")
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
    def __init__(self):
        self._lifetime = ReplayStreamLifetime()
        self._connections = set()
        self._merge_strategy = GreedyMergeStrategy()     # TODO
        self._ended = Event()
        asyncio.ensure_future(self._finalize_after_lifetime_ends())

    async def handle_connection(self, connection):
        try:
            self._lifetime.stream_added()
        except ValueError as e:
            raise StreamEndedError from e
        stream = ConnectionReplayStream(connection, self)
        self._connections.add(connection)
        await self._handle_stream(stream)
        self._connections.remove(connection)
        self._lifetime.stream_removed()

    async def _handle_stream(self, stream):
        try:
            stream.read_header()
        except ValueError:  # TODO
            return  # TODO
        self._merge_strategy.stream_added(stream)
        while not stream.is_complete():
            await stream.read()
            self._merge_strategy.new_data(stream)
        self._merge_strategy.stream_removed(stream)

    def do_not_wait_for_more_connections(self):
        self._lifetime.disable_grace_period()

    def close(self):
        for c in self._connections:
            c.close()

    async def finalize_after_lifetime_ends(self):
        await self._lifetime.ended.wait()
        self._merge_strategy.finalize()
        self._ended.set()

    async def wait_for_ended(self):
        await self._ended.wait()

    @property
    def canonical_stream(self):     # FIXME
        return self._merge_strategy.canonical_stream
