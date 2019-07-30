import pytest
import asyncio
from asynctest.helpers import exhaust_callbacks
from tests import timeout
from replayserver.streams import ReplayStream, ConcreteDataMixin, \
    OutsideSourceReplayStream


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
    s._add_data(b"abc")
    s._header = "Thing"
    assert len(s.data) == 3
    assert s.data.bytes() == b"abc"
    assert s.data[1:2:1] == b"b"
    assert s.header == "Thing"


@pytest.mark.asyncio
@timeout(0.1)
async def test_outside_source_stream_immediate_end(event_loop):
    stream = OutsideSourceReplayStream()
    f1 = asyncio.ensure_future(stream.wait_for_header())
    f2 = asyncio.ensure_future(stream.wait_for_data(0))
    f3 = asyncio.ensure_future(stream.wait_for_ended())
    exhaust_callbacks(event_loop)
    assert not any(x.done() for x in [f1, f2, f3])
    stream.finish()

    assert await f1 is None
    assert await f2 == b""
    assert stream.header is None
    assert stream.data.bytes() == b""
    await f3


@pytest.mark.asyncio
@timeout(0.1)
async def test_outside_source_stream_read_header(event_loop):
    stream = OutsideSourceReplayStream()
    f = asyncio.ensure_future(stream.wait_for_header())
    exhaust_callbacks(event_loop)
    assert not f.done()
    stream.set_header("header")
    assert await f == "header"
    assert stream.header == "header"

    stream.finish()
    await stream.wait_for_ended()
    assert stream.data.bytes() == b""


@pytest.mark.asyncio
@timeout(0.1)
async def test_outside_source_stream_read(event_loop):
    stream = OutsideSourceReplayStream()
    f = asyncio.ensure_future(stream.wait_for_data(0))
    stream.set_header("header")
    await exhaust_callbacks(event_loop)
    assert not f.done()
    stream.feed_data(b"Lorem")
    assert await f == b"Lorem"
    assert stream.data.bytes() == b"Lorem"

    stream.finish()
    await stream.wait_for_ended()


@pytest.mark.asyncio
@timeout(0.1)
async def test_outside_source_stream_immediate_data():
    stream = OutsideSourceReplayStream()
    f1 = asyncio.ensure_future(stream.wait_for_header())
    f2 = asyncio.ensure_future(stream.wait_for_data(0))
    stream.set_header("header")
    stream.feed_data(b"Lorem")
    assert await f1 == "header"
    assert await f2 == b"Lorem"
    assert stream.header == "header"
    assert stream.data.bytes() == b"Lorem"


@pytest.mark.asyncio
@timeout(0.1)
async def test_outside_source_stream_finish():
    stream = OutsideSourceReplayStream()
    f = asyncio.ensure_future(stream.wait_for_data(0))
    stream.finish()
    await f
    assert stream.ended()


@pytest.mark.asyncio
@timeout(0.1)
async def test_outside_source_stream_wait_until_position(event_loop):
    stream = OutsideSourceReplayStream()
    f = asyncio.ensure_future(stream.wait_for_data(3))
    stream.set_header("header")
    stream.feed_data(b"a")
    exhaust_callbacks(event_loop)
    assert not f.done()
    stream.feed_data(b"aa")
    exhaust_callbacks(event_loop)
    assert not f.done()
    stream.feed_data(b"ccc")
    assert await f == b"ccc"


def test_outside_source_stream_discard():
    stream = OutsideSourceReplayStream()
    stream.set_header("header")
    stream.feed_data(b"abcdefgh")
    stream.discard(2)

    with pytest.raises(IndexError):
        stream.data.bytes()
    assert stream.data[2:] == b"cdefgh"
    assert stream.future_data[2:] == b"cdefgh"
    assert stream.data[-2:-1] == b"g"
    assert stream.future_data[-2:-1] == b"g"
    assert stream.data[3] == 100
    assert stream.future_data[3] == 100
    assert len(stream.data) == 8
    assert len(stream.future_data) == 8

    v = stream.data.view(2)
    assert v == b"cdefgh"
    v.release()
