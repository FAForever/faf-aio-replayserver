from enum import Enum
from asyncio.streams import IncompleteReadError, LimitOverrunError


class ReplayConnection:
    class Type(Enum):
        READER = 0
        WRITER = 1

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.position = 0

    async def determine_type(self):
        try:
            prefix = await self.reader.readexactly(2)
            if prefix == b"P/":
                return self.Type.WRITER
            elif prefix == b"G/":
                return self.Type.READER
            else:
                raise ConnectionError("Unexpected data received")
        except IncompleteReadError:
            raise ConnectionError("Disconnected before type was determined")

    async def get_replay_name(self):
        try:
            line = await self.reader.readuntil(b'\0')[:-1].encode()
            uid, name = line.split("/", 1)
            return uid, name
        except IncompleteReadError:
            raise ConnectionError("Disconnected before receiving replay data")
        except (LimitOverrunError, ValueError, UnicodeDecodeError):
            raise ConnectionError("Unexpected data received")

    async def read_replay(self, replay):
        while True:
            data = await self.reader.read(4096)
            if not data:
                break
            await replay.feed(self.position, data)
            self.position += len(data)

    async def write_replay(self, data):
        self.writer.write(data)
        await self.writer.drain()

    async def close(self):
        await self.writer.drain()
        self.writer.close()
        self.reader.close()
