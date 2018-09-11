class MergeStrategy:
    def __init__(self, sink_stream):
        self._streams = set()
        self.sink_stream = sink_stream

    def stream_added(self, stream):
        self.streams.add(stream)

    def stream_removed(self, stream):
        self.streams.remove(stream)

    def new_data(self, stream):
        raise NotImplementedError

    def finalize(self):
        raise NotImplementedError


class GreedyMergeStrategy(MergeStrategy):
    def __init__(self, sink_stream):
        MergeStrategy.__init__(self, sink_stream)

    def stream_added(self, stream):
        MergeStrategy.stream_added(self, stream)
        if self.sink_stream.header is None:
            self.sink_stream.set_header(stream.header)
        self._check_for_new_data(stream)

    def new_data(self, stream):
        self._check_for_new_data(stream)

    def finalize(self):
        self.sink_stream.finish()

    def _check_for_new_data(self, stream):
        canon_len = self.sink_stream.data_length()
        if stream.data_length() <= canon_len:
            return
        self.sink_stream.feed_data(stream.data_from(canon_len))
