import asyncio
from collections import deque


class ReplayTimestamp:
    def __init__(self, stream):
        self._stream = stream

    async def timestamps(self, delay):
        stamps = deque([0], maxlen=delay)
        while not self._stream.is_complete():
            stamps.append(self._stream.data_length())
            yield stamps[0]
            await asyncio.sleep(1)
        yield self._stream.data_length()
