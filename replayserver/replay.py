import asyncio
from replayserver.replaysender import ReplaySender
from replayserver.replaystream import ReplayStream


class Replay:
    def __init__(self):
        self._loop = asyncio.get_event_loop()
        self.stream = ReplayStream(self._loop)
        self.sender = ReplaySender(self.stream, self._loop)

    def close(self):
        self.stream.close()
        self.sender.close()

    async def wait_for_data_complete(self):
        await self.stream.wait_for_ended()

    async def wait_for_ended(self):
        await self.stream.wait_for_ended()
        await self.sender.wait_for_ended()
