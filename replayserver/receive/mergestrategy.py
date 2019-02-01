from enum import Enum
import asyncio
from replayserver.logging import logger
from replayserver import config


class MergeStrategies(Enum):
    GREEDY = "GREEDY"
    FOLLOW_STREAM = "FOLLOW_STREAM"

    def build(self, sink_stream, config):
        if self == MergeStrategies.GREEDY:
            return GreedyMergeStrategy.build(sink_stream, config)
        elif self == MergeStrategies.FOLLOW_STREAM:
            return FollowStreamMergeStrategy.build(sink_stream, config)

    def config(self, config):
        if self == MergeStrategies.GREEDY:
            return GreedyMergeStrategyConfig(config.with_namespace("greedy"))
        elif self == MergeStrategies.FOLLOW_STREAM:
            return FollowStreamMergeStrategyConfig(
                config.with_namespace("follow_stream"))


class MergeStrategy:
    def __init__(self, sink_stream):
        self.sink_stream = sink_stream

    def new_header(self, stream):
        raise NotImplementedError

    def new_data(self, stream):
        raise NotImplementedError

    def finalize(self):
        raise NotImplementedError

    # An added stream will always start with no header and no data.
    def stream_added(self, stream):
        raise NotImplementedError

    def stream_removed(self, stream):
        raise NotImplementedError


class GreedyMergeStrategyConfig(config.Config):
    pass


class GreedyMergeStrategy(MergeStrategy):
    """
    Greedily takes data from the first stream that has it.
    """
    def __init__(self, sink_stream):
        MergeStrategy.__init__(self, sink_stream)

    @classmethod
    def build(cls, sink_stream, config):
        return cls(sink_stream)

    def stream_added(self, stream):
        pass

    def stream_removed(self, stream):
        pass

    def new_header(self, stream):
        if self.sink_stream.header is None:
            self.sink_stream.set_header(stream.header)

    def new_data(self, stream):
        canon_len = len(self.sink_stream.data)
        if len(stream.data) <= canon_len:
            return
        self.sink_stream.feed_data(stream.data[canon_len:])

    def finalize(self):
        self.sink_stream.finish()


class DivergenceTracking:
    """
    Allows us to compare stream with sink for divergence. Ensures that we never
    compare the same data twice.
    """
    def __init__(self, stream, sink):
        self._stream = stream
        self._sink = sink
        self.diverges = False
        self._compared_num = 0

    def check_divergence(self):
        if self.diverges:
            return
        start = self._compared_num
        end = min(len(self._stream.data), len(self._sink.data))
        if start >= end:
            return
        # This is safe since we don't await with the memoryviews
        with memoryview(self._stream.data.bytes()) as view1, \
                memoryview(self._sink.data.bytes()) as view2:
            self.diverges = view1[start:end] != view2[start:end]
        self._compared_num = end


class FollowStreamMergeStrategyConfig(config.Config):
    _options = {
        "stall_check_period": {
            "parser": config.positive_float,
            "doc": ("Time in seconds after which, if the followed connection "
                    "did not produce data, another connection is selected.")
        }
    }


class FollowStreamMergeStrategy(MergeStrategy):
    """
    Tries its best to follow a single stream. If it ends, looks for a new
    stream to track, ensuring first that it has matching data.
    This strategy guarantees that the sink will equal a stream which is not a
    prefix of any other (a "maximal" stream). It does NOT guarantee that a most
    common stream will be picked, so if a replay we track diverges and
    terminates early, tough luck. It does, however, protect against a stream
    that stalls by periodically checking if any new data has been sent and
    switching to another stream if it hasn't.
    This is roughly the strategy of the original replay server.

    Invariants:
    0. Streams match iif one's data is a prefix of the other's.
    1. Tracked stream always matches and has at least as much data as sink.
    2. We don't track a stream iif no stream fits above condition.
    3. Matching set MUST contain all matching streams.
    4. Matching set MAY contain diverging streams. They are lazily removed
       when we check if a stream is fit to be tracked.
    5. After finalize(), ALL streams either diverge or are prefices of sink.
    """
    def __init__(self, sink_stream, config):
        MergeStrategy.__init__(self, sink_stream)
        self._candidates = {}
        self._tracked_value = None
        self._stalling_watchdog = asyncio.ensure_future(
            self._guard_against_stalling(config.stall_check_period))

    @classmethod
    def build(cls, sink_stream, config):
        return cls(sink_stream, config)

    @property
    def _tracked(self):
        return self._tracked_value

    @_tracked.setter
    def _tracked(self, val):
        if self._tracked_value is not None:
            logger.debug(f"Stopped tracking {self._tracked}")
        self._tracked_value = val
        if self._tracked_value is not None:
            logger.debug(f"Started tracking {self._tracked}")

    def _is_ahead_of_sink(self, stream):
        return len(stream.data) > len(self.sink_stream.data)

    def _eligible_for_tracking(self, stream):
        if not self._is_ahead_of_sink(stream):
            return False
        self._check_for_divergence(stream)
        return stream in self._candidates

    def _check_for_divergence(self, stream):
        check = self._candidates[stream]
        check.check_divergence()
        if check.diverges:
            logger.debug(f"{stream} diverges from canonical stream, removing")
            del self._candidates[stream]

    def _stream_has_diverged(self, stream):
        return stream not in self._candidates

    def _feed_sink(self):
        if self._tracked is None:
            return
        sink_len = len(self.sink_stream.data)
        self.sink_stream.feed_data(self._tracked.data[sink_len:])

    def _find_new_stream(self):
        for stream in list(self._candidates.keys()):
            if self._eligible_for_tracking(stream):
                self._tracked = stream
                break
        self._feed_sink()

    def stream_added(self, stream):
        self._candidates[stream] = DivergenceTracking(stream, self.sink_stream)

    def stream_removed(self, stream):
        if self._stream_has_diverged(stream):
            return
        # Don't remove a not-tracked stream - it might have more data that
        # matches currently tracked stream, in case it ends short!
        if stream is self._tracked:
            self._tracked = None
            self._candidates.pop(stream, None)
            self._find_new_stream()

    def new_data(self, stream):
        if self._stream_has_diverged(stream):
            return
        if self._tracked is None and self._eligible_for_tracking(stream):
            self._tracked = stream
        if stream is self._tracked:
            self._feed_sink()

    def finalize(self):
        self._stalling_watchdog.cancel()
        # Check any ended streams we saved for later
        while self._tracked is not None:
            self.stream_removed(self._tracked)
        self._candidates.clear()
        self.sink_stream.finish()

    def new_header(self, stream):
        if self.sink_stream.header is None:
            self.sink_stream.set_header(stream.header)

    async def _guard_against_stalling(self, stall_check_period):
        """
        Stops tracking a stream if it didn't advance for stall_check_period
        seconds, possibly finding a better one.
        This will always let us advance further if possible - the stream we
        just removed won't get picked again, since a stream needs to be
        strictly ahead of the sink to be eligible (and a tracked stream is
        always equal with it). Either we'll immediately pick a stream that's
        further ahead or won't track until first eligible stream appears.
        """
        current_pos = len(self.sink_stream.data)
        previous_pos = current_pos
        while True:
            previous_pos = current_pos
            await asyncio.sleep(stall_check_period)
            current_pos = len(self.sink_stream.data)
            if current_pos == previous_pos and self._tracked is not None:
                logger.debug((f"{self._tracked} has been stalling for "
                              f"{stall_check_period}s - stopping tracking"))
                self._tracked = None
                self._find_new_stream()
