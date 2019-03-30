from replayserver.stream import ReplayStream, ConcreteDataMixin


def test_data_uses_right_stream_methods():
    class TestReplayStream(ReplayStream):
        def __init__(self):
            ReplayStream.__init__(self)

        def _data_length(self):
            return 3

        def _data_bytes(self):
            return b"abc"

        def _data_slice(self, s):
            return b"b"

    s = TestReplayStream()
    assert len(s.data) == 3
    assert s.data.bytes() == b"abc"
    assert s.data[1:2:1] == b"b"


def test_concrete_mixin():
    class TestConcreteDataMixinStream(ConcreteDataMixin, ReplayStream):
        def __init__(self):
            ConcreteDataMixin.__init__(self)
            ReplayStream.__init__(self)

    s = TestConcreteDataMixinStream()
    s._data += b"abc"
    s._header = "Thing"
    assert len(s.data) == 3
    assert s.data.bytes() == b"abc"
    assert s.data[1:2:1] == b"b"
    assert s.header == "Thing"
