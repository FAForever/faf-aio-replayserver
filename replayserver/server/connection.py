from enum import Enum
import asyncio
from asyncio.streams import IncompleteReadError, LimitOverrunError
from replayserver.errors import MalformedDataError, EmptyConnectionError
from replayserver.logging import short_exc


class Connection:
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self._closed = False
        self._header = None
        self._closed_by_us = False

    async def read(self, size):
        try:
            data = await self.reader.read(size)
            return data
        except ConnectionError as e:
            raise MalformedDataError(f"Connection error: {short_exc(e)}")

    async def readuntil(self, delim):
        try:
            return await self.reader.readuntil(delim)
        except (IncompleteReadError, LimitOverrunError):
            raise MalformedDataError(f"Failed to find {delim} in read data")
        except ConnectionError as e:
            raise MalformedDataError(f"Connection error: {short_exc(e)}")

    async def readexactly(self, amount):
        try:
            return await self.reader.readexactly(amount)
        except IncompleteReadError:
            raise MalformedDataError(
                f"Stream ended while reading exactly {amount}")
        except ConnectionError as e:
            raise MalformedDataError(f"Connection error: {short_exc(e)}")

    async def write(self, data):
        if self._closed:
            return False
        try:
            self.writer.write(data)
            await self.writer.drain()
        except ConnectionResetError:
            return False
        except (TimeoutError, ConnectionError, OSError) as e:
            raise MalformedDataError(f"Connection error: {short_exc(e)}")
        return True

    def close(self):
        self._closed = True
        self._closed_by_us = True
        self.writer.close()
        # Reader and writer share a transport, so no need to close reader.

    async def wait_closed(self):
        try:
            await self.writer.wait_closed()
        except (ConnectionError, TimeoutError, OSError):
            pass

    def closed_by_us(self):
        return self._closed_by_us

    def add_header(self, header):
        self._header = header

    def __str__(self):
        if self._header is None:
            return f"Initial connection, id {id(self)}"
        else:
            return f"{self._header}"


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
    async def read(cls, connection, timeout):
        try:
            return await asyncio.wait_for(cls._do_read(connection), timeout)
        except asyncio.TimeoutError:
            raise MalformedDataError("Timed out while reading header")

    @classmethod
    async def _do_read(cls, connection):
        type_ = await cls._read_type(connection)
        game_id, game_name = await cls._read_game_data(connection)
        return cls(type_, game_id, game_name)

    @classmethod
    async def _read_type(cls, connection):
        try:
            prefix = await connection.readexactly(2)
        except MalformedDataError:
            # Vast majority of these will be connections that entered lobby,
            # but quit without starting the game. This also ignores very early
            # connection errors and reads of length exactly 1; consider that a
            # FIXME.
            raise EmptyConnectionError
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
