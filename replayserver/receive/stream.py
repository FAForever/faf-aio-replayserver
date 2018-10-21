from replayserver.stream import ReplayStream, ConcreteDataMixin, \
    DataEventMixin, HeaderEventMixin, EndedEventMixin
from replayserver.struct.header import ReplayHeader
from replayserver.errors import MalformedDataError


class ConnectionReplayStream(ConcreteDataMixin, DataEventMixin,
                             HeaderEventMixin, EndedEventMixin, ReplayStream):
    def __init__(self, header_reader, connection):
        ConcreteDataMixin.__init__(self)
        DataEventMixin.__init__(self)
        HeaderEventMixin.__init__(self)
        EndedEventMixin.__init__(self)
        ReplayStream.__init__(self)

        self._header_reader = header_reader
        self._connection = connection
        self._leftovers = b""

    @classmethod
    def build(cls, connection, **kwargs):
        header_reader = ReplayHeader.from_connection
        return cls(header_reader, connection)

    async def read_header(self):
        try:
            result = await self._header_reader(self._connection)
            # Don't add leftover data right away, caller doesn't expect that
            self._header, self._leftovers = result
        except MalformedDataError:
            self._end()
            raise
        finally:
            self._signal_header_read_or_ended()

    async def read(self):
        if self._leftovers:
            data = self._leftovers
            self._leftovers = b""
        else:
            data = await self._connection.read(4096)
            if not data:
                self._end()
        self._data += data
        self._signal_new_data_or_ended()


class OutsideSourceReplayStream(ConcreteDataMixin, DataEventMixin,
                                HeaderEventMixin, EndedEventMixin,
                                ReplayStream):
    def __init__(self):
        ConcreteDataMixin.__init__(self)
        DataEventMixin.__init__(self)
        HeaderEventMixin.__init__(self)
        EndedEventMixin.__init__(self)
        ReplayStream.__init__(self)

    def set_header(self, header):
        self._header = header
        self._signal_header_read_or_ended()

    def feed_data(self, data):
        self._data += data
        self._signal_new_data_or_ended()

    def finish(self):
        self._end()
        self._signal_header_read_or_ended()
        self._signal_new_data_or_ended()
