import asyncio
from asyncio.locks import Event
import math
from collections import deque


class Timestamp:
    def __init__(self, stream, interval, delay):
        self._stream = stream
        self._interval = interval
        self._delay = delay

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

    async def timestamps(self):
        while not self._stream.ended():
            await self._new_stamp.wait()
            yield self._stamps[0]
