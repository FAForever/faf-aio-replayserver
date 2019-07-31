import asyncio
from asyncio.locks import Event
import math
from collections import deque
from replayserver.streams.base import ReplayStream


class Timestamp:
    def __init__(self, stream, interval, delay):
        self._stream = stream
        self._interval = interval
        self._delay = delay
        self._ended = False

        # Last item in deque size n+1 is from n intervals ago
        stamp_number = math.ceil(self._delay / self._interval) + 1
        self._stamps = deque([0], maxlen=stamp_number)
        self._new_stamp = Event()
        self._stamp_coro = asyncio.ensure_future(self._periodic_stamp())
        asyncio.ensure_future(self._wait_for_stream_end())

    def _stamp(self, pos):
        self._stamps.append(pos)
        self._new_stamp.set()
        self._new_stamp.clear()

    async def _periodic_stamp(self):
        while True:
            self._stamp(len(self._stream.data))
            await asyncio.sleep(self._interval)

    async def _wait_for_stream_end(self):
        await self._stream.wait_for_ended()
        self._stamp_coro.cancel()
        self._stamps.clear()
        self._stamp(len(self._stream.data))
        self._ended = True

    async def timestamps(self):
        while not self._ended:
            await self._new_stamp.wait()
            yield self._stamps[0]
        yield self._stamps[0]


class DelayedReplayStream(ReplayStream):
    def __init__(self, stream, timestamp):
        ReplayStream.__init__(self)
        self._stream = stream
        self._timestamp = timestamp
        self._current_position = 0
        asyncio.ensure_future(self._track_delayed_stream())

    @classmethod
    def build(cls, stream, config):
        timestamp = Timestamp(stream,
                              config.update_interval,
                              config.replay_delay)
        return cls(stream, timestamp)

    @property
    def header(self):
        return self._stream.header

    def _data_length(self):
        return min(len(self._stream.data), self._current_position)

    def _data_slice(self, s):
        if isinstance(s, slice):
            return self._get_slice(s)
        else:
            if s >= self._current_position:
                raise IndexError
            elif s < 0:
                diff = len(self._stream.data) - self._current_position
                s -= diff
            return self._stream.data[s]

    def _get_slice(self, s):
        s, e, st = s.indices(self._current_position)
        st = min(st, self._current_position)
        return self._stream.data[slice(s, e, st)]

    def _data_bytes(self):
        if self.ended():
            return self._stream.data.bytes()
        else:
            return self._stream.data[:self._current_position]

    def _data_view(self, start, end):
        cutoff = self._current_position - start
        return self._stream.data.view(start, end)[:cutoff]

    def _future_data_length(self):
        return len(self._stream.future_data)

    def _future_data_slice(self, v):
        return self._stream.future_data[v]

    def _future_data_bytes(self):
        return self._stream.future_data.bytes()

    def _future_data_view(self, start, end):
        return self._stream.future_data.view(start, end)

    def discard(self, until):
        return self._stream.discard(until)

    async def _track_delayed_stream(self):
        await self._stream.wait_for_header()
        self._header_available()
        await self._track_data()
        self._end()

    async def _track_data(self):
        async for position in self._timestamp.timestamps():
            if position <= self._current_position:
                continue
            self._current_position = position
            self._data_available()
