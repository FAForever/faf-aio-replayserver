import asyncio
from asyncio.locks import Event
from contextlib import contextmanager

from replayserver.errors import CannotAcceptConnectionError
from replayserver.receive.stream import ConnectionReplayStream, \
    OutsideSourceReplayStream


class GracePeriod:
    def __init__(self, grace_period_time):
        self._grace_period_time = grace_period_time
        self._ended = Event()
        self._grace_period = None

    def is_over(self):
        return self._ended.is_set()

    def disable(self):
        self._grace_period_time = 0
        if self._grace_period is not None:
            self.stop()
            self.start()

    def start(self):
        if self._grace_period is not None:
            return
        self._grace_period = asyncio.ensure_future(self._grace_period_wait())

    def stop(self):
        if self._grace_period is None:
            return
        self._grace_period.cancel()
        self._grace_period = None

    async def elapsed(self):
        await self._ended.wait()

    async def _grace_period_wait(self):
        await asyncio.sleep(self._grace_period_time)
        self._ended.set()


class Merger:
    def __init__(self, stream_builder, grace_period_time, merge_strategy,
                 canonical_stream):
        self._stream_builder = stream_builder
        self._end_grace_period = GracePeriod(grace_period_time)
        self._merge_strategy = merge_strategy
        self.canonical_stream = canonical_stream
        self._stream_count = 0
        self._ended = Event()
        asyncio.ensure_future(self._finalize_after_ending())
        # In case no connections arrive at all, we still want to end
        # (e.g. exception between replay creation and reaching the merger
        self._end_grace_period.start()

    @classmethod
    def build(cls, *, config_merger_grace_period_time,
              config_replay_merge_strategy, **kwargs):
        canonical_replay = OutsideSourceReplayStream()
        merge_strategy = config_replay_merge_strategy.builder(
            canonical_replay, **kwargs)
        stream_builder = ConnectionReplayStream.build
        return cls(stream_builder, config_merger_grace_period_time,
                   merge_strategy, canonical_replay)

    @contextmanager
    def _stream_lifetime(self, connection):
        stream = self._stream_builder(connection)
        self._merge_strategy.stream_added(stream)
        self._stream_count += 1
        self._end_grace_period.stop()
        try:
            yield stream
        finally:
            self._merge_strategy.stream_removed(stream)
            self._stream_count -= 1
            if self._stream_count == 0:
                self._end_grace_period.start()

    async def handle_connection(self, connection):
        if self._end_grace_period.is_over():
            raise CannotAcceptConnectionError(
                "Writer connection arrived after replay writing finished")
        with self._stream_lifetime(connection) as stream:
            await stream.read_header()
            self._merge_strategy.new_header(stream)
            while not stream.ended():
                await stream.read()
                self._merge_strategy.new_data(stream)

    def close(self):
        self._end_grace_period.disable()

    async def _finalize_after_ending(self):
        await self._end_grace_period.elapsed()
        self._merge_strategy.finalize()
        self.canonical_stream.finish()
        self._ended.set()

    async def wait_for_ended(self):
        await self._ended.wait()
