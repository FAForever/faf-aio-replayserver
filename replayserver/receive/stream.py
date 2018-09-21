from replayserver.stream import ReplayStream, ConcreteDataMixin, \
    DataEventMixin, HeaderEventMixin
from replayserver.struct.header import ReplayHeader
from replayserver.errors import MalformedDataError


class ConnectionReplayStream(ConcreteDataMixin, DataEventMixin,
                             HeaderEventMixin, ReplayStream):
    def __init__(self, header_reader, connection):
        ConcreteDataMixin.__init__(self)
        DataEventMixin.__init__(self)
        HeaderEventMixin.__init__(self)
        ReplayStream.__init__(self)

        self._header_reader = header_reader
        self._connection = connection
        self._ended = False

    @classmethod
    def build(cls, connection):
        header_reader = ReplayHeader.generator()
        return cls(header_reader, connection)

    async def read_header(self):
        try:
            self._header, leftovers = await self._feed_header_reader()
            self._data += leftovers
        except MalformedDataError as e:
            self._ended = True
            raise
        finally:
            self._signal_new_data_or_ended()
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
        data = await self._connection.read(4096)
        if not data:
            self._ended = True
        self._data += data
        self._signal_new_data_or_ended()

    def ended(self):
        return self._ended


class OutsideSourceReplayStream(ConcreteDataMixin, DataEventMixin,
                                HeaderEventMixin, ReplayStream):
    def __init__(self):
        ConcreteDataMixin.__init__(self)
        DataEventMixin.__init__(self)
        HeaderEventMixin.__init__(self)
        ReplayStream.__init__(self)
        self._ended = False

    def set_header(self, header):
        self._header = header
        self._signal_header_read_or_ended()

    def feed_data(self, data):
        self._data += data
        self._signal_new_data_or_ended()

    def finish(self):
        self._ended = True
        self._signal_header_read_or_ended()
        self._signal_new_data_or_ended()

    def ended(self):
        return self._ended
