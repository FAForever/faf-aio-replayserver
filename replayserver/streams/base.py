"""
Abstract classes for manipulating data streams as replays.
"""

from asyncio.locks import Event


class ReplayStreamData:
    """
    Data buffer wrapper that allows to avoid unnecessary copies.
    """

    def __init__(self, length, slice_, bytes_, view):
        self._length = length
        self._slice = slice_
        self._bytes = bytes_
        self._view = view

    def __len__(self):
        return self._length()

    def __getitem__(self, val):
        return self._slice(val)

    def bytes(self):
        return self._bytes()

    def view(self, start=None, end=None):
        return self._view(start, end)


class ReplayStream:
    """
    Abstract class representing a stream of replay data. Allows users to wait
    until the header is available, or wait for more data to arrive. Also lets
    them access the header and the data. Probably a poor man's analogue of
    Java's Input/OutputStream.

    A concrete implementation can cause new data to arrive in various ways
    (e.g. reading from network, merging other streams, being a delay layer to
    another stream), therefore we do not define any interface for adding new
    data here.

    The data is available as a "data" member. Since some implementations could
    want to withhold some data from the underlying buffer without keeping a
    copy, it's not accessed directly as a bytes object, but through stream
    methods. Methods supported are len, slicing, accessing all data via a
    bytes() method (trying to avoid a copy if possible) and taking
    a memoryview with a view(start, end) method (which shouldn't result in any
    copies, but has to be released before awaiting on anything).

    For those users that want to peek at future data that was withheld (e.g.
    merge strategies), a future_data member is a available that supports the
    same methods as data and should give access to all received data.

    A replay stream can optionally implement a discard() method for dropping
    replay data until some position. This can result in major memory savings,
    but accessing deleted ranges is a programmer error. In particular:

    * Calling bytes() on a stream that had discard() called on it can throw an
      IndexError.
    * Slicing and calling memoryiew with start out of range can cut off data.
    * len() is not affected by discarding bytes.
    * You can discard data until length of 'future_data', beyond length of
      'data'. Discarding beyond that is an error.
    """

    def __init__(self):
        self.data = ReplayStreamData(self._data_length, self._data_slice,
                                     self._data_bytes, self._data_view)
        self.future_data = ReplayStreamData(self._future_data_length,
                                            self._future_data_slice,
                                            self._future_data_bytes,
                                            self._future_data_view)
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
        create a new buffer each time.
        """
        raise NotImplementedError

    def _data_view(self, start, end):
        """
        Returns a memoryview of current data from start to end. Should not
        create any copies, MUST be released before awaiting on anything.
        """
        raise NotImplementedError

    def _future_data_length(self):
        return self._data_length()

    def _future_data_slice(self, s):
        return self._data_slice(s)

    def _future_data_bytes(self):
        return self._data_bytes()

    def _future_data_view(self, start, end):
        return self._data_view(start, end)

    def discard(self, until):
        """
        Discards stream data until position.
        """
        raise ValueError

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
        self._discarded_data = 0

    @property
    def header(self):
        return self._header

    def _data_length(self):
        return self._discarded_data + len(self._data)

    def _data_slice(self, s):
        if isinstance(s, slice):
            return self._get_slice(s)
        if s >= 0:
            if s < self._discarded_data:
                raise IndexError
            s -= self._discarded_data
        return self._data[s]

    def _get_slice(self, s):
        s, e, st = s.start, s.stop, s.step
        if s is None and self._discarded_data > 0:
            raise IndexError

        if s is not None and s >= 0:
            if s < self._discarded_data:
                raise IndexError
            s -= self._discarded_data
        if e is not None and e >= 0:
            if e < self._discarded_data:
                raise IndexError
            e -= self._discarded_data

        return self._data[slice(s, e, st)]

    def _data_bytes(self):
        if self._discarded_data > 0:
            raise ValueError
        return self._data

    def discard(self, until):
        if until <= self._discarded_data:
            return

        diff = until - self._discarded_data
        if diff > len(self._data):
            raise IndexError

        del self._data[:diff]
        self._discarded_data = until

    def _data_view(self, start, end):
        if start is None:
            if self._discarded_data > 0:
                raise IndexError
            else:
                start = 0
        if end is None:
            end = self._data_length()

        start -= self._discarded_data
        end -= self._discarded_data
        if start < 0 or end < 0:
            raise IndexError
        return memoryview(self._data)[start:end]


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
