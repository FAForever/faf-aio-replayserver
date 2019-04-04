from replayserver.stream import OutsideSourceReplayStream
from replayserver.struct.header import ReplayHeader
from replayserver.struct.mangling import StreamDemangler
from replayserver.errors import MalformedDataError


class ReplayStreamReader:
    def __init__(self, header_reader, demangler, stream, connection):
        self._header_reader = header_reader
        self._demangler = demangler
        self._connection = connection
        self._leftovers = b""

        # This is public - you can use it even after you discard the reader.
        self.stream = stream

    @classmethod
    def build(cls, connection):
        header_reader = ReplayHeader.from_connection
        demangler = StreamDemangler()
        stream = OutsideSourceReplayStream()
        return cls(header_reader, demangler, stream, connection)

    async def _read_header(self):
        try:
            result = await self._header_reader(self._connection)
            # Don't add leftover data right away, caller doesn't expect that
            header, self._leftovers = result
            self.stream.set_header(header)
        except MalformedDataError:
            self.stream.finish()
            raise

    async def _read_data(self):
        if self._leftovers:
            data = self._leftovers
            self._leftovers = b""
            self.stream.feed_data(data)
            return

        try:
            data = await self._connection.read(4096)
        except MalformedDataError:
            # Connection might be unusable now, but stream's data so far is
            # still valid and useful. End safely and let future code throw
            # if it tries to use the connection.
            data = b""

        if not data:
            data = self._demangler.drain()
            self.stream.feed_data(data)
            self.stream.finish()
        else:
            data = self._demangler.demangle(data)
            self.stream.feed_data(data)

    async def read(self):
        "Guarantees to finish the stream, no matter if it throws."
        await self._read_header()
        while not self.stream.ended():
            await self._read_data()
