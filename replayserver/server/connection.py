from enum import Enum
from asyncio.streams import IncompleteReadError, LimitOverrunError
from replayserver.errors import MalformedDataError


class Connection:
    class Type(Enum):
        READER = 0
        WRITER = 1

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.type = None
        self.uid = None
        self.name = None

    async def read_header(self):
        await self._determine_type()
        await self._get_replay_name()

    async def _determine_type(self):
        try:
            prefix = await self.reader.readexactly(2)
            if prefix == b"P/":
                self.type = self.Type.WRITER
            elif prefix == b"G/":
                self.type = self.Type.READER
            else:
                raise MalformedDataError(
                    f"Expected reader or writer prefix, got '{prefix}'")
        except IncompleteReadError as e:
            raise MalformedDataError(
                f"EOF before conn type, got '{e.partial}'")

    async def _get_replay_name(self):
        try:
            line = await self.reader.readuntil(b'\0')
            line = line[:-1].decode()
            self.uid, self.name = line.split("/", 1)
            self.uid = int(self.uid)
        except IncompleteReadError as e:
            raise MalformedDataError(
                f"EOF before connection header, got '{e.partial[:100]}'")
        except (LimitOverrunError, ValueError, UnicodeDecodeError):
            raise MalformedDataError("Malformed connection header")

    async def read(self, size):
        data = await self.reader.read(size)
        return data

    async def write(self, data):
        self.writer.write(data)
        await self.writer.drain()   # TODO - might hurt performance?

    def close(self):
        self.writer.transport.abort()   # Drop connection immediately
        # We don't need to close reader (according to docs?)