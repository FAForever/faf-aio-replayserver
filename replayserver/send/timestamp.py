import asyncio
import math
from collections import deque


class Timestamp:
    def __init__(self, stream, interval, delay):
        self._stream = stream
        self._interval = interval
        self._delay = delay

    async def timestamps(self):
        # Last item in deque size n+1 is from n intervals ago
        stamp_number = math.ceil(self._delay / self._interval) + 1
        stamps = deque([0], maxlen=stamp_number)
        while not self._stream.ended():
            stamps.append(len(self._stream.data))
            yield stamps[0]
            await asyncio.sleep(self._interval)
        yield len(self._stream.data)
