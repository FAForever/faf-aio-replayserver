import asyncio

from replayserver.struct.header import ReplayHeader
from replayserver.errors import MalformedDataError
from replayserver.common import ServesConnections
from replayserver.streams import OutsideSourceReplayStream, DelayedReplayStream
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
            if len(data) < 4096:
                await asyncio.sleep(1)

    async def read(self):
        "Guarantees to finish the stream, no matter if it throws."
        await self._read_header()
        while not self.stream.ended():
            await self._read_data()


class MergerConfig(config.Config):
    _options = {
        "desired_quorum": {
            "parser": config.positive_int,
            "doc": ("The number of writers that have to agree on a piece of "
                    "data before it's sent to readers. HIGHLY recommended to "
                    "set this to 2. Setting it to 3 or higher might prevent "
                    "cutting a replay short in a marginal number of cases"
                    ", but can cost more time comparing stream data.")
        },
        "stream_comparison_cutoff": {
            "parser": lambda x: None if x == "" else config.positive_int(x),
            "default": "",
            "doc": ("Maximum number of bytes used for comparing streams to a "
                    "merged stream. If set to x, then any streams will only "
                    "be compared x bytes back.\n\n"
                    ""
                    "The merge strategy defers comparing streams between one "
                    "another until a stream is needed to resolve conflicts. "
                    "Setting a cutoff prevents the merger from comparing an "
                    "entire stream to data merged so far. This saves CPU time "
                    "and a LOT of memory by discarding data beyond cutoff, at "
                    "a risk of using a diverged stream to merge data.\n\n"
                    ""
                    "The above doesn't happen in practice - a diverged replay "
                    "stays diverged (and ends soon after).")
        }
    }


class DelayConfig(config.Config):
    _options = {
        "replay_delay": {
            "parser": config.positive_float,
            "doc": ("Delay in seconds between receiving replay data and "
                    "sending it to readers. Used to prevent cheating via "
                    "playing and observing at the same time.\n\n"
                    "Note that current replay stream merging strategy relies "
                    "on having a buffer of future data in can compare "
                    "between streams! It's highly recommended to set this to "
                    "a reasonably high value (e.g. five minutes).")
        },
        "update_interval": {
            "parser": config.positive_float,
            "doc": ("Frequency, in seconds, of checking for new data to send "
                    "to listeners. This affects frequency of calling send() "
                    "of listener sockets, as the server sets high/low buffer "
                    "water marks to 0 in order to prevent unwanted latency. "
                    "Setting this value higher might improve performance.")
        }
    }


class Merger(ServesConnections):
    def __init__(self, reader_builder, delayed_stream_builder,
                 merge_strategy, canonical_stream):
        ServesConnections.__init__(self)
        self._reader_builder = reader_builder
        self._delayed_stream_builder = delayed_stream_builder
        self._merge_strategy = merge_strategy
        self.canonical_stream = canonical_stream

    @classmethod
    def build(cls, merge_config, delay_config):
        canonical_replay = OutsideSourceReplayStream()
        merge_strategy = QuorumMergeStrategy.build(canonical_replay,
                                                   merge_config)
        return cls(ReplayStreamReader.build,
                   lambda s: DelayedReplayStream.build(s, delay_config),
                   merge_strategy, canonical_replay)

    async def _handle_connection(self, connection):
        reader = self._reader_builder(connection)
        delayed_stream = self._delayed_stream_builder(reader.stream)
        strategy_callbacks = asyncio.ensure_future(
            self._merge_strategy.track_stream(delayed_stream))
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
