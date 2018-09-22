import asyncio
import math
from collections import deque


class Timestamp:
    def __init__(self, stream, interval, delay):
        self._stream = stream
        self._interval = interval
        self._delay = delay

    async def timestamps(self):
        stamp_number = math.ceil(self._delay / self._interval)
        stamps = deque([0], maxlen=stamp_number)
        while not self._stream.is_complete():
            stamps.append(self._stream.data_length())
            yield stamps[0]
            await asyncio.sleep(self._interval)
        yield self._stream.data_length()
