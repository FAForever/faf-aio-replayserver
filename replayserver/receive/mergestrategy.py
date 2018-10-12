from enum import Enum


class MergeStrategies(Enum):
    GREEDY = "GREEDY"
    FOLLOW_STREAM = "FOLLOW_STREAM"

    def build(self, *args, **kwargs):
        if self == MergeStrategies.GREEDY:
            return GreedyMergeStrategy.build(*args, **kwargs)
        elif self == MergeStrategies.FOLLOW_STREAM:
            return FollowStreamMergeStrategy.build(*args, **kwargs)


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


class GreedyMergeStrategy(MergeStrategy):
    """
    Greedily takes data from the first stream that has it.
    """
    def __init__(self, sink_stream):
        MergeStrategy.__init__(self, sink_stream)

    @classmethod
    def build(cls, sink_stream, **kwargs):
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


class FollowStreamMergeStrategy(MergeStrategy):
    """
    Tries its best to follow a single stream. If it ends, looks for a new
    stream to track, ensuring first that it has matching data.
    This strategy guarantees that the sink will equal a stream which is not a
    prefix of any other (a "maximal" stream). It does NOT guarantee that a most
    common stream will be picked, so if a replay we track diverges and
    terminates early, tough luck. It also does not protect against a stream
    that stalls.
    This is the strategy of the original replay server.

    Invariants:
    0. Streams match iif one's data is a prefix of the other's.
    1. Tracked stream MUST match sink at the moment of feeding data.
    2. If stream is set as tracked, it MUST match sink at that moment.
    3. Matching set MUST contain all matching streams at all times.
    4. Matching set MUST contain tracked stream at all times.
    5. Matching set MAY contain diverging streams, but all streams in matching
       set MUST match sink at moment of adding.
    6. After finalize(), ALL streams either diverge or are prefices of sink.
    """
    def __init__(self, sink_stream):
        MergeStrategy.__init__(self, sink_stream)
        # We keep info on where to we compared a stream to sink last
        self._matching_streams = {}
        self._tracked_stream = None

    @classmethod
    def build(cls, sink_stream, **kwargs):
        return cls(sink_stream)

    def _matching(self, stream):
        return stream in self._matching_streams

    def _cmp_end(self, stream):
        return min(len(stream.data), len(self.sink_stream.data))

    def _matches_sink(self, stream):
        start = self._matching_streams[stream]
        end = self._cmp_end(stream)
        if start >= end:
            return True
        # This is safe since we don't await with the memoryviews
        with memoryview(stream.data.bytes()) as view1, \
                memoryview(self.sink_stream.data.bytes()) as view2:
            return view1[start:end] == view2[start:end]

    def _check_for_divergence(self, stream):
        if self._matches_sink(stream):
            self._matching_streams[stream] = self._cmp_end(stream)
        else:
            del self._matching_streams[stream]

    def _feed_tracked_data(self):
        tracked = self._tracked_stream
        if tracked is None:
            return
        sink_len = len(self.sink_stream.data)
        if len(tracked.data) <= sink_len:
            return

        # In case the stream diverged before it reached sink length
        self._check_for_divergence(tracked)
        if not self._matching(tracked):
            # Won't happen if we're called from _find_new_stream,
            # so no worries about recursion here
            self._find_new_stream()
            return
        self.sink_stream.feed_data(tracked.data[sink_len:])

    def _find_new_stream(self):
        self._tracked_stream = None
        for stream in list(self._matching_streams.keys()):
            self._check_for_divergence(stream)
            if self._matching(stream):
                self._tracked_stream = stream
        self._feed_tracked_data()

    def stream_added(self, stream):
        self._matching_streams[stream] = 0  # No data for now, so it matches
        if self._tracked_stream is None:
            self._tracked_stream = stream
            self._feed_tracked_data()

    def stream_removed(self, stream):
        # Don't remove a not-tracked stream - it might have more data that
        # matches currently tracked stream, in case it ends short!
        if stream is self._tracked_stream:
            self._matching_streams.pop(stream, None)
            self._find_new_stream()

    def new_data(self, stream):
        if stream is self._tracked_stream:
            self._feed_tracked_data()

    def finalize(self):
        # Check any ended streams we saved for later
        while self._matching_streams:
            self._feed_tracked_data()
            self.stream_removed(self._tracked_stream)
        self.sink_stream.finish()

    def new_header(self, stream):
        if self.sink_stream.header is None:
            self.sink_stream.set_header(stream.header)
