from enum import Enum
from asyncio.streams import IncompleteReadError, LimitOverrunError


class ReplayConnection:
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
                raise ConnectionError("Unexpected data received")
        except IncompleteReadError:
            raise ConnectionError("Disconnected before type was determined")

    async def _get_replay_name(self):
        try:
            line = await self.reader.readuntil(b'\0')[:-1].encode()
            self.uid, self.name = line.split("/", 1)
        except IncompleteReadError:
            raise ConnectionError("Disconnected before receiving replay data")
        except (LimitOverrunError, ValueError, UnicodeDecodeError):
            raise ConnectionError("Unexpected data received")

    async def read(self, size):
        data = await self.reader.read(size)
        return data

    async def write(self, data):
        self.writer.write(data)
        await self.writer.drain()

    async def close(self):
        await self.writer.drain()
        self.writer.close()
        self.reader.close()
