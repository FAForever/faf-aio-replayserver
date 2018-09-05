import asyncio
from asyncio.locks import Event
from io import BytesIO
from replayserver.errors import StreamEndedError
from replayserver.replaystream import ReplayStream


class ReplayMerger:
    GRACE_PERIOD = 30

    def __init__(self, loop):
        self._loop = loop
        self._streams = set()
        self._ended = Event(loop=self._loop)
        self._grace_period = None

        self.position = 0
        self.data = bytearray()

    @property
    def ended(self):
        return self._ended.is_set()

    def add_writer(self, writer):
        if self.ended:
            raise StreamEndedError("Tried to add a writer to an ended stream!")
        stream = ReplayStream(writer, self)
        self._streams.add(stream)
        if self._grace_period is not None:
            self._grace_period.cancel()
            self._grace_period = None
        stream.start_read_future()

    def remove_stream(self, stream):
        self._writers.remove(stream)
        if not self.writers:
            if self._grace_period is not None:
                self._grace_period.cancel()
            self._grace_period = asyncio.ensure_future(
                self._no_streams_grace_period(), loop=self.loop)

    def stream_ended(self, stream):
        self.remove_stream(stream)

    async def _no_streams_grace_period(self):
        await asyncio.sleep(self.GRACE_PERIOD, loop=self.loop)
        self.close()

    def close(self):
        for w in self._streams:
            w.close()
        self._ended.set()

    # TODO - use strategies here
    async def on_new_data(self, stream):
        if len(self.data) >= stream.position:
            return
        self.data += stream.data[len(self.data):]

    def get_data(self, start, end):
        return self.data[start:end]

    async def wait_for_ended(self):
        await self._ended.wait()
