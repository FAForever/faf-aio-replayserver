import asyncio
from replayserver.stream import ReplayStream, DataEventMixin
from replayserver.send.timestamp import Timestamp


class DelayedReplayStream(DataEventMixin, ReplayStream):
    DELAY = 300
    INTERVAL = 1

    def __init__(self, stream, timestamp):
        DataEventMixin.__init__(self)
        ReplayStream.__init__(self)
        self._stream = stream
        self._timestamp = timestamp
        self._current_position = 0
        self._ended = False
        asyncio.ensure_future(self._track_current_position())

    @classmethod
    def build(cls, stream):
        timestamp = Timestamp(stream, cls.INTERVAL, cls.DELAY)
        return cls(stream, timestamp)

    @property
    def header(self):
        return self._stream.header

    async def wait_for_header(self):
        return (await self._stream.wait_for_header())

    def _data_length(self):
        return min(len(self._stream.data), self._current_position)

    def _data_slice(self, s):
        if s.stop is None:
            new_stop = self._current_position
        else:
            new_stop = min(s.stop, self._current_position)
        return self._stream.data[slice(s.start, new_stop, s.step)]

    def _data_bytes(self):
        return self._stream.data[:self._current_position]

    def ended(self):
        return self._ended

    async def _track_current_position(self):
        async for position in self._timestamp.timestamps():
            if position <= self._current_position:
                continue
            self._current_position = position
            self._signal_new_data_or_ended()
        self._ended = True
        self._signal_new_data_or_ended()
