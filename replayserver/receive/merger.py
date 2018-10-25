import asyncio
from asyncio.locks import Event
from contextlib import contextmanager

from replayserver.errors import CannotAcceptConnectionError
from replayserver.collections import AsyncCounter
from replayserver.receive.stream import ConnectionReplayStream, \
    OutsideSourceReplayStream


class MergerLifetime:
    def __init__(self, count, grace_period):
        self._count = count
        self._grace_period = grace_period
        self._force_end = Event()

    async def wait(self):
        await self._wait_for_any(self._no_connections_for_a_while(),
                                 self._force_end.wait())

    async def _no_connections_for_a_while(self):
        while True:
            await self._count.wait_until_empty()
            if (await self._grace_period_without_connections()):
                return

    async def _grace_period_without_connections(self):
        try:
            await asyncio.wait_for(self._count.wait_until_not_empty(),
                                   timeout=self._grace_period)
            return False
        except asyncio.TimeoutError:
            return True

    async def _wait_for_any(self, *coros):
        _, p = await asyncio.wait(coros, return_when=asyncio.FIRST_COMPLETED)
        for task in p:
            task.cancel()

    def force_end(self):
        self._force_end.set()


class Merger:
    def __init__(self, stream_builder, grace_period_time, merge_strategy,
                 canonical_stream):
        self._stream_builder = stream_builder
        self._stream_count = AsyncCounter()
        self._lifetime = MergerLifetime(self._stream_count, grace_period_time)
        self._merge_strategy = merge_strategy
        self.canonical_stream = canonical_stream
        self._closing = False
        self._ended = Event()
        asyncio.ensure_future(self._finalize_after_ending())

    @classmethod
    def build(cls, *, config_merger_grace_period_time,
              config_replay_merge_strategy, **kwargs):
        canonical_replay = OutsideSourceReplayStream()
        merge_strategy = config_replay_merge_strategy.build(
            canonical_replay, **kwargs)
        stream_builder = ConnectionReplayStream.build
        return cls(stream_builder, config_merger_grace_period_time,
                   merge_strategy, canonical_replay)

    @contextmanager
    def _stream_lifetime(self, connection):
        stream = self._stream_builder(connection)
        self._merge_strategy.stream_added(stream)
        self._stream_count.inc()
        try:
            yield stream
        finally:
            self._merge_strategy.stream_removed(stream)
            self._stream_count.dec()

    async def handle_connection(self, connection):
        if self._closing:
            raise CannotAcceptConnectionError(
                "Writer connection arrived after replay writing finished")
        with self._stream_lifetime(connection) as stream:
            await stream.read_header()
            self._merge_strategy.new_header(stream)
            while not stream.ended():
                await stream.read()
                self._merge_strategy.new_data(stream)

    def close(self):
        self._lifetime.force_end()

    async def _finalize_after_ending(self):
        await self._lifetime.wait()
        self._closing = True
        await self._stream_count.wait_until_empty()

        self._merge_strategy.finalize()
        self.canonical_stream.finish()
        self._ended.set()

    async def wait_for_ended(self):
        await self._ended.wait()
