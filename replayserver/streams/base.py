"""
Abstract classes for manipulating data streams as replays.
"""

from asyncio.locks import Event


class ReplayStreamData:
    """
    Data buffer wrapper that allows to avoid unnecessary copies.
    """
    def __init__(self, stream):
        self._stream = stream

    def __len__(self):
        return self._stream._data_length()

    def __getitem__(self, val):
        if not isinstance(val, slice):
            raise ValueError
        return self._stream._data_slice(val)

    def bytes(self):
        return self._stream._data_bytes()


class ReplayStream:
    """
    Abstract class representing a stream of replay data. Allows users to wait
    until the header is available, or wait for more data to arrive. Also lets
    them access the header and the data.

    A concrete implementation can cause new data to arrive in various ways
    (e.g. reading from network, merging other streams, being a delay layer to
    another stream), therefore we do not define any interface for adding new
    data here.

    The data is available as a "data" member. Since some implementations could
    want to withhold some data from the underlying buffer without keeping a
    copy, it's not a raw bytes object, but a proxy you can slice and convert to
    bytes to get the whole underlying buffer. This way we can avoid copying the
    whole proxied buffer every time we need a small slice.
    """

    def __init__(self):
        self.data = ReplayStreamData(self)
        self._ended = Event()
        self._header_read_or_ended = Event()
        self._new_data_or_ended = Event()

    @property
    def header(self):
        """
        None until the header is successfully read. Note that you can't tell if
        reading the header failed until the stream (eventually) ends.
        If set, MUST be a ReplayHeader instance.
        """
        pass

    def _data_length(self):
        "Current data length."
        raise NotImplementedError

    def _data_slice(self, s):
        "Given a slice, returns sliced data."
        raise NotImplementedError

    def _data_bytes(self):
        """
        Returns byte buffer with entire stream data. For some subclasses it may
        create a new buffer each time, for these you should avoid calling this
        too often.
        """
        raise NotImplementedError

    def _header_available(self):
        "Called by implementation once header is available."
        self._header_read_or_ended.set()

    def _data_available(self):
        "Called by implementation when more data is available."
        self._new_data_or_ended.set()
        self._new_data_or_ended.clear()

    def _end(self):
        "Called by implementation when the replay is over."
        self._ended.set()
        self._header_read_or_ended.set()
        self._new_data_or_ended.set()

    # Public-facing part of interface starts here.
    async def wait_for_header(self):
        await self._header_read_or_ended.wait()
        return self.header

    async def wait_for_data(self, position):
        """
        Wait until there is data after specified position. Return the new data,
        or bytes() if the stream ended and there is no data past position.
        """
        while position >= len(self.data) and not self.ended():
            await self._new_data_or_ended.wait()
        if position < len(self.data):
            return self.data[position:]
        else:
            return b""

    def ended(self):
        """
        Whether the stream is finished processing. MUST return True if there
        was an error reading the stream (e.g. while reading the header).
        """
        return self._ended.is_set()

    async def wait_for_ended(self):
        await self._ended.wait()


class ConcreteDataMixin:
    def __init__(self):
        self._header = None
        self._data = bytearray()

    @property
    def header(self):
        return self._header

    def _data_length(self):
        return len(self._data)

    def _data_slice(self, s):
        return self._data[s]

    def _data_bytes(self):
        return self._data


class OutsideSourceReplayStream(ConcreteDataMixin, ReplayStream):
    def __init__(self):
        ConcreteDataMixin.__init__(self)
        ReplayStream.__init__(self)

    def set_header(self, header):
        self._header = header
        self._header_available()

    def feed_data(self, data):
        self._data += data
        self._data_available()

    def finish(self):
        self._end()
