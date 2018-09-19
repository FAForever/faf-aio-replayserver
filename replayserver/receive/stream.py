from asyncio.locks import Condition

from replayserver.stream import ConcreteReplayStream
from replayserver.struct.header import ReplayHeader
from replayserver.errors import MalformedDataError


class ConnectionReplayStream(ConcreteReplayStream):
    def __init__(self, header_reader, connection):
        ConcreteReplayStream.__init__(self)
        self._header_reader = header_reader
        self._connection = connection
        self._finished = False

    @classmethod
    def build(cls, connection):
        header_reader = ReplayHeader.generator()
        return cls(header_reader, connection)

    async def read_header(self):
        if self.header is not None:
            return
        while not self._header_reader.done():
            data = await self._connection.read(4096)
            if not data:
                raise MalformedDataError("Replay header ended prematurely")
            try:
                self._header_reader.send(data)
            except ValueError:
                # TODO - more informative logs
                raise MalformedDataError("Malformed replay header")
        self.header, leftovers = self._header_reader.result()
        self.data += leftovers

    async def read(self):
        data = await self._connection.read(4096)
        if not data:
            self._finished = True
        self.data += data

    def is_complete(self):
        return self._finished


class OutsideSourceReplayStream(ConcreteReplayStream):
    def __init__(self):
        ConcreteReplayStream.__init__(self)
        self._finished = False
        self._new_data = Condition()

    async def _wait_for(self, condition):
        async with self._new_data:
            await self._new_data.wait_for(condition)

    async def read_header(self):
        await self._wait_for(lambda: self.header is not None
                             or self.is_complete())
        if self.header is None:
            raise MalformedDataError(
                "Source did not provide header before ending stream")
        return self.header

    def set_header(self, header):
        self.header = header
        self._new_data.notify_all()

    async def read(self):
        current_len = self.data_length()
        await self._wait_for(lambda: current_len < self.data_length()
                             or self.is_complete())
        return

    def feed_data(self, data):
        self.data += data
        self._new_data.notify_all()

    def is_complete(self):
        return self._finished

    def finish(self):
        self._finished = True
