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
        header_reader = ReplayHeader.generator()
        return cls(header_reader, connection)

    async def read_header(self):
        try:
            # We avoid appending leftover data right away - it would force the
            # caller to make extra checks after reading header, to make sure it
            # didn't miss data. It's easier to guarantee here that first read()
            # always starts from 0.
            self._header, self._leftovers = await self._feed_header_reader()
        except MalformedDataError as e:
            self._end()
            raise
        finally:
            self._signal_header_read_or_ended()

    async def _feed_header_reader(self):
        while not self._header_reader.done():
            data = await self._connection.read(4096)
            if not data:
                raise MalformedDataError("Replay header ended prematurely")
            try:
                self._header_reader.send(data)
            except ValueError:
                raise MalformedDataError("Malformed replay header")
        return self._header_reader.result()

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
