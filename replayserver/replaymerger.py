import asyncio
from asyncio.locks import Event
from replayserver.errors import StreamEndedError
from replayserver.replaystream import ReplayStream


class ReplayStreamLifetime:
    GRACE_PERIOD = 30

    def __init__(self):
        self._streams = set()
        self.ended = Event()
        self._grace_period = None

    def add_stream(self, stream):
        self._streams.add(stream)
        self._cancel_grace_period()

    def remove_stream(self, stream):
        self._streams.discard(stream)
        if not self._streams:
            self._start_grace_period()

    def end(self):
        self._streams.clear()
        self._cancel_grace_period()
        self.ended.set()

    def _cancel_grace_period(self):
        if self._grace_period is not None:
            self._grace_period.cancel()
            self._grace_period = None

    def _start_grace_period(self):
        if self._grace_period is None:
            self._grace_period = asyncio.ensure_future(
                self._no_streams_grace_period())

    async def _no_streams_grace_period(self):
        await asyncio.sleep(self.GRACE_PERIOD, loop=self.loop)
        self.ended.set()


class ReplayMerger:
    def __init__(self, loop):
        self._loop = loop
        self._streams = set()
        self._lifetime = ReplayStreamLifetime()
        self.position = 0
        self.data = bytearray()

    @property
    def ended(self):
        return self._lifetime.ended.is_set()

    def add_writer(self, writer):
        if self.ended:
            raise StreamEndedError("Tried to add a writer to an ended stream!")
        stream = ReplayStream(writer, self)
        self._lifetime.add_stream(stream)
        self._streams.add(stream)
        stream.start_read_future()

    def close(self):
        for w in self._streams:
            w.close()
        self._lifetime.end()

    # TODO - use strategies here
    async def on_new_data(self, stream):
        if len(self.data) >= stream.position:
            return
        self.data += stream.data[len(self.data):]

    def get_data(self, start, end):
        return self.data[start:end]

    async def wait_for_ended(self):
        await self._lifetime.ended.wait()
