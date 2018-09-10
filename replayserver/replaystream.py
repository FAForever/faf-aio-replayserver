import asyncio
from asyncio.locks import Condition
from replayserver.replaytimestamp import ReplayTimestamp


class ReplayStream:
    """
    Has to be called before read_data.
    """
    async def read_header(self):
        raise NotImplementedError

    async def read(self):
        raise NotImplementedError

    def data_length(self):
        raise NotImplementedError

    def data_from(self, position):
        raise NotImplementedError

    def is_complete(self):
        raise NotImplementedError

    async def read_data(self, position):
        while position >= self.data_length() and not self.is_complete():
            await self.read()
        return self.data_from(position)


class ConcreteReplayStream:
    def __init__(self):
        ReplayStream.__init__(self)
        self.header = None
        self.data = bytearray()

    def data_length(self):
        return len(self.data)

    def data_from(self, position):
        return self.data[position:]


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


class DelayedReplayStream(ReplayStream):
    DELAY = 300

    def __init__(self, stream):
        self._stream = stream
        self._current_position = 0
        self._finished = False
        self._new_data = Condition()
        self._timestamp = ReplayTimestamp(self._stream)
        asyncio.ensure_future(self._track_current_position)

    async def read_header(self):
        return (await self._stream.read_header())

    async def read(self):
        if not self.is_complete():
            async with self._new_data:
                await self._new_data.wait()

    def data_length(self):
        return self._current_position

    def data_from(self, position):
        return self._stream.data[position:self._current_position]

    def is_complete(self):
        return self._finished

    async def _track_current_position(self):
        async for position in self._timestamp.timestamps(self.DELAY):
            if position <= self._current_position:
                continue
            self._current_position = position
            self._new_data.notify_all()
        self._finished = True
        self._new_data.notify_all()
