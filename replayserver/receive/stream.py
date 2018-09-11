from replayserver.stream import ConcreteReplayStream
from asyncio.locks import Condition


class ConnectionReplayStream(ConcreteReplayStream):
    def __init__(self, connection):
        ConcreteReplayStream.__init__(self)
        self._connection = connection
        self._finished = False

    async def read_header(self):
        pass    # TODO

    async def read(self):
        data = self._connection.read(4096)
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
            raise ValueError    # FIXME
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
