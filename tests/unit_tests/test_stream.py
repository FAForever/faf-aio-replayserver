import pytest
import asyncio
from tests import timeout
from asynctest.helpers import exhaust_callbacks

from replayserver.stream import ReplayStream, HeaderEventMixin, \
    DataEventMixin, ConcreteDataMixin


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


class HeaderMixinStream(HeaderEventMixin, ReplayStream):
    def __init__(self):
        HeaderEventMixin.__init__(self)
        ReplayStream.__init__(self)
        self._header = None
        self._ended = False

    @property
    def header(self):
        return self._header

    def ended(self):
        return self._ended


@pytest.mark.asyncio
@timeout(0.1)
async def test_header_mixin_waits_on_header(event_loop):
    s = HeaderMixinStream()
    f = asyncio.ensure_future(s.wait_for_header())
    exhaust_callbacks(event_loop)
    assert not f.done()
    s._header = "Thing"
    s._signal_header_read_or_ended()
    head = await f
    assert head == "Thing"


@pytest.mark.asyncio
@timeout(0.1)
async def test_header_mixin_waits_on_ended(event_loop):
    s = HeaderMixinStream()
    f = asyncio.ensure_future(s.wait_for_header())
    exhaust_callbacks(event_loop)
    assert not f.done()
    s._ended = True
    s._signal_header_read_or_ended()
    head = await f
    assert head is None


@pytest.mark.asyncio
@timeout(0.1)
async def test_header_mixin_immediate_return(event_loop):
    s = HeaderMixinStream()
    s._header = "Thing"
    header = await s.wait_for_header()
    assert header == "Thing"

    s = HeaderMixinStream()
    s._ended = True
    header = await s.wait_for_header()
    assert header is None


class DataMixinStream(DataEventMixin, ReplayStream):
    def __init__(self):
        DataEventMixin.__init__(self)
        ReplayStream.__init__(self)
        self._data = b""
        self._ended = False

    def _data_length(self):
        return len(self._data)

    def _data_bytes(self):
        return self._data

    def _data_slice(self, s):
        return self._data[s]

    def ended(self):
        return self._ended


@pytest.mark.asyncio
@timeout(0.1)
async def test_data_mixin_waits_on_data(event_loop):
    s = DataMixinStream()
    f = asyncio.ensure_future(s.wait_for_data())
    exhaust_callbacks(event_loop)
    assert not f.done()
    s._data += b"a"
    s._signal_new_data_or_ended()
    data = await f
    assert data == b"a"


@pytest.mark.asyncio
@timeout(0.1)
async def test_data_mixin_waits_on_ended(event_loop):
    s = DataMixinStream()
    f = asyncio.ensure_future(s.wait_for_data())
    exhaust_callbacks(event_loop)
    assert not f.done()
    s._ended = True
    s._signal_new_data_or_ended()
    data = await f
    assert data == b""


@pytest.mark.asyncio
@timeout(0.1)
async def test_data_mixin_immediate_data():
    s = DataMixinStream()
    f = asyncio.ensure_future(s.wait_for_data())
    s._data += b"a"
    s._signal_new_data_or_ended()
    data = await f
    assert data == b"a"


@pytest.mark.asyncio
@timeout(0.1)
async def test_data_mixin_wait_until_position(event_loop):
    s = DataMixinStream()
    f = asyncio.ensure_future(s.wait_for_data(3))
    s._data += b"a"
    s._signal_new_data_or_ended()
    exhaust_callbacks(event_loop)
    assert not f.done()
    s._data += b"aab"
    s._signal_new_data_or_ended()
    data = await f
    assert data == b"b"


@pytest.mark.asyncio
@timeout(0.1)
async def test_data_mixin_immediate_return(event_loop):
    s = DataMixinStream()
    s._data += b"a"
    data = await s.wait_for_data(0)
    assert data == b"a"

    s = DataMixinStream()
    s._ended = True
    data = await s.wait_for_data()
    assert data == b""


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
