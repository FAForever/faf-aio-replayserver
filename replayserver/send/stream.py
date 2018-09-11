import asyncio
from asyncio.locks import Condition
from replayserver.stream import ReplayStream
from replayserver.send.timestamp import Timestamp


class DelayedReplayStream(ReplayStream):
    DELAY = 300

    def __init__(self, stream):
        self._stream = stream
        self._current_position = 0
        self._finished = False
        self._new_data = Condition()
        self._timestamp = Timestamp(self._stream)
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
