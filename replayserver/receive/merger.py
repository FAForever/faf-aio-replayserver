import asyncio
from asyncio.locks import Event
from contextlib import contextmanager

from replayserver.common import CanStopServingConnsMixin
from replayserver.errors import CannotAcceptConnectionError
from replayserver.receive.stream import ConnectionReplayStream, \
    OutsideSourceReplayStream
from replayserver.receive.mergestrategy import MergeStrategies
from replayserver import config


class MergerConfig(config.Config):
    _options = {
        "strategy": {
            "parser": MergeStrategies,
            "doc": ("Replay merge strategy to use. Available strategies are "
                    "GREEDY and FOLLOW_STREAM (recommended).")
        }
    }

    def __init__(self, config):
        super().__init__(config)
        strat_config = config.with_namespace("strategy_config")
        self.strategy_config = self.strategy.config(strat_config)


class Merger(CanStopServingConnsMixin):
    def __init__(self, stream_builder, merge_strategy, canonical_stream):
        CanStopServingConnsMixin.__init__(self)
        self._stream_builder = stream_builder
        self._merge_strategy = merge_strategy
        self.canonical_stream = canonical_stream
        self._ended = Event()
        asyncio.ensure_future(self._lifetime())

    @classmethod
    def build(cls, config):
        canonical_replay = OutsideSourceReplayStream()
        merge_strategy = config.strategy.build(canonical_replay,
                                               config.strategy_config)
        stream_builder = ConnectionReplayStream.build
        return cls(stream_builder, merge_strategy, canonical_replay)

    @contextmanager
    def _stream_tracking(self, connection):
        stream = self._stream_builder(connection)
        self._merge_strategy.stream_added(stream)
        self._connection_count.inc()
        try:
            yield stream
        finally:
            self._merge_strategy.stream_removed(stream)
            self._connection_count.dec()

    async def handle_connection(self, connection):
        if not self._accepts_connections():
            raise CannotAcceptConnectionError(
                "Merger no longer accepts connections")
        with self._stream_tracking(connection) as stream:
            await stream.read_header()
            self._merge_strategy.new_header(stream)
            while not stream.ended():
                await stream.read()
                self._merge_strategy.new_data(stream)

    async def no_connections_for(self, grace_period):
        await self._connection_count.wait_until_empty_for(grace_period)

    async def _lifetime(self):
        await self._wait_until_all_connections_end()
        self._merge_strategy.finalize()
        self.canonical_stream.finish()
        self._ended.set()

    async def wait_for_ended(self):
        await self._ended.wait()
