from replayserver.replaystream import OutsideSourceReplayStream


class MergeStrategy:
    def __init__(self):
        self._streams = set()
        self.canonical_stream = OutsideSourceReplayStream()

    def stream_added(self, stream):
        self.streams.add(stream)

    def stream_removed(self, stream):
        self.streams.remove(stream)

    def new_data(self, stream):
        raise NotImplementedError

    def finalize(self):
        raise NotImplementedError


class GreedyMergeStrategy(MergeStrategy):
    def __init__(self):
        MergeStrategy.__init__(self)

    def stream_added(self, stream):
        MergeStrategy.stream_added(self, stream)
        if self.canonical_stream.header is None:
            self.canonical_stream.set_header(stream.header)
        self._check_for_new_data(stream)

    def new_data(self, stream):
        self._check_for_new_data(stream)

    def finalize(self):
        self.canonical_stream.finish()

    def _check_for_new_data(self, stream):
        if self.canonical_stream.is_complete():
            return
        canon_len = self.canonical_stream.data_length()
        if stream.data_length() <= canon_len:
            return
        self.canonical_stream.feed_data(stream.data_from(canon_len))
