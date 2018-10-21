from asyncio.locks import Event


class ReplayStreamData:
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
    This abstract class represents a stream of replay data. It allows users of
    this class to wait until the header is available, or wait for more data to
    arrive. It also lets them access the header and the data.

    A concrete implementation can cause new data to arrive in various ways
    (e.g. reading from network, merging other streams, being a delay layer to
    another stream), therefore we do not define any interface for adding new
    data here.

    The data is available as a "data" member. Since some implementations could
    want to withhold some data from the underlying buffer without keeping a
    copy, it's not a raw bytes object, but a proxy you can slice and convert to
    bytes to get the whole underlying buffer. This way we can avoid copying the
    whole proxied buffer every time we need a small slice.
    ( TODO - is the above premature optimization? )
    """

    def __init__(self):
        self.data = ReplayStreamData(self)

    @property
    def header(self):
        """
        None until the header is successfully read. Note that you can't tell if
        reading the header failed until the stream (eventually) ends.
        If set, MUST be a ReplayHeader instance.
        """
        pass

    async def wait_for_header(self):
        """ Wait until either the header was read or the stream ended. """
        raise NotImplementedError

    def _data_length(self):
        raise NotImplementedError

    def _data_slice(self, s):
        raise NotImplementedError

    def _data_bytes(self, s):
        raise NotImplementedError

    async def wait_for_data(self, position=None):
        """
        Wait until there is data after current position (or 'position', if
        specified). Return the new data, or bytes() if the stream ended and
        there is no data past position.

        Be aware that this should also work if it's run via ensure_future()!
        Make sure you note down current position immediately when called or
        you can end up missing appended data!
        """
        raise NotImplementedError

    def ended(self):
        """
        Whether the stream is finished processing. MUST return True if there
        was an error reading the stream (e.g. while reading the header).
        """
        raise NotImplementedError

    async def wait_for_ended(self):
        raise NotImplementedError


class HeaderEventMixin:
    """ Useful when the class adds header via a coroutine. """
    def __init__(self):
        self._header_read_or_ended = Event()

    def _signal_header_read_or_ended(self):
        self._header_read_or_ended.set()

    async def wait_for_header(self):
        await self._header_read_or_ended.wait()
        return self.header


class DataEventMixin:
    """ Useful when the class adds data via a coroutine. """
    def __init__(self):
        self._new_data_or_ended = Event()

    def _signal_new_data_or_ended(self):
        self._new_data_or_ended.set()
        self._new_data_or_ended.clear()

    def wait_for_data(self, position=None):
        if position is None:
            position = len(self.data)
        return self._wait_for_data(position)

    async def _wait_for_data(self, position=None):
        while position >= len(self.data) and not self.ended():
            await self._new_data_or_ended.wait()
        if position < len(self.data):
            return self.data[position:]
        if self.ended():
            return b""


class EndedEventMixin:
    """ Useful when the class adds header via a coroutine. """
    def __init__(self):
        self._ended = Event()

    def _end(self):
        self._ended.set()

    def ended(self):
        return self._ended.is_set()

    async def wait_for_ended(self):
        await self._ended.wait()


class ConcreteDataMixin:
    """ Useful when the class holds the data instead of proxying it. """
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
