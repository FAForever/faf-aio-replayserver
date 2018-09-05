import asyncio
from asyncio.locks import Condition


class ReplayTimestamp:
    def __init__(self, stream, loop, delay):
        self._stream = stream
        self._loop = loop
        self._delay = delay
        self._stamps = []
        self._final_stamp = None
        self._stamper = asyncio.ensure_future(self.stamp(), loop=self._loop)
        self._new_stamp = Condition()

    async def stamp(self):
        while True:
            # Ensure these two happen atomically
            self._stamps.append(self._stream.position)
            if self._stream.ended:
                self._final_stamp = len(self._stamps)

            async with self._new_stamp:
                self._new_stamp.notify_all()
            if self._final_stamp is not None:
                return
            await asyncio.sleep(1)

    def stop(self):
        self._stamper.cancel()

    async def next_stamp(self, stamp_idx):
        if stamp_idx == self._final_stamp:
            return None

        # If replay has ended, send rest of it without delay
        if self._final_stamp is not None:
            return self._final_stamp

        def available():
            return stamp_idx < len(self._stamps) - self._delay
        async with self._new_stamp:
            await self._new_stamp.wait_for(available)
        return len(self._stamps)

    def position(self, idx):  # Moved by 1 to make math easier
        try:
            return self._stamps[idx - 1]
        except IndexError:
            return 0
