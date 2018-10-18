from enum import Enum
from asyncio.streams import IncompleteReadError, LimitOverrunError
from replayserver.errors import MalformedDataError


class Connection:
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    async def read(self, size):
        data = await self.reader.read(size)
        return data

    async def readuntil(self, delim):
        try:
            return await self.reader.readuntil(delim)
        except (IncompleteReadError, LimitOverrunError) as e:
            raise MalformedDataError(f"Failed to find {delim} in read data")

    async def readexactly(self, amount):
        try:
            return await self.reader.readexactly(amount)
        except IncompleteReadError:
            raise MalformedDataError(
                f"Stream ended while reading exactly {amount}")

    async def write(self, data):
        self.writer.write(data)

    def close(self):
        self.writer.transport.abort()   # Drop connection immediately
        # We don't need to close reader (according to docs?)


class ConnectionHeader:
    class Type(Enum):
        READER = "reader"
        WRITER = "writer"

    def __init__(self, type_, game_id, game_name):
        self.type = type_
        self.game_id = game_id
        self.game_name = game_name

    def __str__(self):
        return f"{self.type.value} for {self.game_id} ({self.game_name})"

    @classmethod
    async def read(cls, connection):
        type_ = await cls._read_type(connection)
        game_id, game_name = await cls._read_game_data(connection)
        return cls(type_, game_id, game_name)

    @classmethod
    async def _read_type(cls, connection):
        prefix = await connection.readexactly(2)
        if prefix == b"P/":
            return cls.Type.WRITER
        elif prefix == b"G/":
            return cls.Type.READER
        else:
            raise MalformedDataError(
                f"Expected reader or writer prefix, got '{prefix}'")

    @classmethod
    async def _read_game_data(cls, connection):
        try:
            line = await connection.readuntil(b'\0')
            line = line[:-1].decode()
            game_id, game_name = line.split("/", 1)
            i, n = int(game_id), game_name
            if i < 0:
                raise MalformedDataError("Negative game ID!")
            return i, n
        except (ValueError, UnicodeDecodeError):
            raise MalformedDataError("Malformed connection header")
