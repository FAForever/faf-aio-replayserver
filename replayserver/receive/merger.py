import asyncio
from asyncio.locks import Event

from replayserver.common import ServesConnections
from replayserver.errors import CannotAcceptConnectionError
from replayserver.receive.stream import ReplayStreamReader, \
    OutsideSourceReplayStream
from replayserver.receive.mergestrategy import FollowStreamMergeStrategy
from replayserver import config


class MergerConfig(config.Config):
    _options = {
        "stall_check_period": {
            "parser": config.positive_float,
            "doc": ("Time in seconds after which, if the followed connection "
                    "did not produce data, another connection is selected.")
        }
    }


class Merger(ServesConnections):
    def __init__(self, reader_builder, merge_strategy, canonical_stream):
        ServesConnections.__init__(self)
        self._reader_builder = reader_builder
        self._merge_strategy = merge_strategy
        self.canonical_stream = canonical_stream

    @classmethod
    def build(cls, config):
        canonical_replay = OutsideSourceReplayStream()
        merge_strategy = FollowStreamMergeStrategy.build(canonical_replay,
                                                         config)
        stream_builder = ReplayStreamReader.build
        return cls(stream_builder, merge_strategy, canonical_replay)

    async def _handle_connection(self, connection):
        reader = self._reader_builder(connection)
        strategy_callbacks = asyncio.ensure_future(
            self._merge_strategy.track_stream(reader.stream))
        try:
            await reader.read()
        finally:
            await strategy_callbacks
        return reader.stream

    async def no_connections_for(self, grace_period):
        await self._connection_count.wait_until_empty_for(grace_period)

    async def _after_connections_end(self):
        self._merge_strategy.finalize()
        self.canonical_stream.finish()

    def __str__(self):
        return "Merger"
