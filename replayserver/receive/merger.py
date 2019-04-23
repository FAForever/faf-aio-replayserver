import asyncio

from replayserver.struct.header import ReplayHeader
from replayserver.errors import MalformedDataError
from replayserver.common import ServesConnections
from replayserver.streams import OutsideSourceReplayStream
from replayserver.receive.mergestrategy import QuorumMergeStrategy
from replayserver import config


class ReplayStreamReader:
    def __init__(self, header_reader, stream, connection):
        self._header_reader = header_reader
        self._connection = connection
        self._leftovers = b""

        # This is public - you can use it even after you discard the reader.
        self.stream = stream

    @classmethod
    def build(cls, connection):
        header_reader = ReplayHeader.from_connection
        stream = OutsideSourceReplayStream()
        return cls(header_reader, stream, connection)

    async def _read_header(self):
        try:
            result = await self._header_reader(self._connection)
            # Don't add leftover data right away, caller doesn't expect that
            header, self._leftovers = result
            self.stream.set_header(header)
        except MalformedDataError:
            self.stream.finish()
            raise

    async def _read_data(self):
        if self._leftovers:
            data = self._leftovers
            self._leftovers = b""
            self.stream.feed_data(data)
            return

        try:
            data = await self._connection.read(4096)
        except MalformedDataError:
            # Connection might be unusable now, but stream's data so far is
            # still valid and useful. End safely and let future code throw
            # if it tries to use the connection.
            data = b""

        if not data:
            self.stream.finish()
        else:
            self.stream.feed_data(data)

    async def read(self):
        "Guarantees to finish the stream, no matter if it throws."
        await self._read_header()
        while not self.stream.ended():
            await self._read_data()


class MergerConfig(config.Config):
    _options = {
        "desired_quorum": {
            "parser": config.positive_int,
            "doc": ("The number of writers that have to agree on a piece of"
                    "data before it's sent to readers. HIGHLY recommended to"
                    "set this to 2. Setting it to 3 or higher might prevent"
                    "cutting a replay short in a marginal number of cases"
                    ", but can cost more time comparing stream data.")
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
        merge_strategy = QuorumMergeStrategy.build(canonical_replay,
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
