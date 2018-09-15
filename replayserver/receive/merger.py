import asyncio
from asyncio.locks import Event
from contextlib import contextmanager

from replayserver.errors import CannotAcceptConnectionError
from replayserver.receive.stream import ConnectionReplayStream, \
    OutsideSourceReplayStream
from replayserver.receive.mergestrategy import GreedyMergeStrategy


class StreamLifetime:
    GRACE_PERIOD = 30

    def __init__(self):
        self._stream_count = 0
        self.ended = Event()
        self._grace_period = None
        self._grace_period_enabled = True

    def is_over(self):
        return self.ended.is_set()

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


class Merger:
    def __init__(self, lifetime, merge_strategy, canonical_stream):
        self._lifetime = lifetime
        self._merge_strategy = merge_strategy
        self.canonical_stream = canonical_stream
        self._ended = Event()
        asyncio.ensure_future(self._finalize_after_lifetime_ends())

    @classmethod
    def build(cls):
        lifetime = StreamLifetime()
        canonical_replay = OutsideSourceReplayStream()
        merge_strategy = GreedyMergeStrategy(canonical_replay)
        return cls(lifetime, merge_strategy, canonical_replay)

    @contextmanager
    def _get_stream(self, connection):
        stream = ConnectionReplayStream(connection, self)
        self._connections.add(connection)
        self._lifetime.stream_added()
        try:
            yield stream
        finally:
            self._connections.remove(connection)
            self._lifetime.stream_removed()

    async def handle_connection(self, connection):
        if self._lifetime.is_over():
            raise CannotAcceptConnectionError(
                "Writer connection arrived after replay writing finished")
        with self._get_stream(connection) as stream:
            await self._handle_stream(stream)

    async def _handle_stream(self, stream):
        await stream.read_header()
        self._merge_strategy.stream_added(stream)
        while not stream.is_complete():
            await stream.read()
            self._merge_strategy.new_data(stream)
        self._merge_strategy.stream_removed(stream)

    def close(self):
        self._lifetime.disable_grace_period()
        for c in self._connections:
            c.close()

    async def finalize_after_lifetime_ends(self):
        await self._lifetime.ended.wait()
        self._merge_strategy.finalize()
        self._ended.set()

    async def wait_for_ended(self):
        await self._ended.wait()
