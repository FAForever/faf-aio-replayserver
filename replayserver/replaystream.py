import asyncio
from asyncio.locks import Event
from io import BytesIO
from replayserver.errors import StreamEndedError


class ReplayStream:
    GRACE_PERIOD = 30

    def __init__(self, loop):
        self._loop = loop
        self._writers = set()
        self._write_futures = set()
        self._ended = Event(loop=self.loop)
        self._grace_period = None

        self.position = 0
        self.data = BytesIO()

    @property
    def ended(self):
        return self._ended.is_set()

    def add_writer(self, writer):
        if self.ended:
            raise StreamEndedError("Tried to add a writer to an ended stream!")
        self._writers.add(writer)
        if self._grace_period is not None:
            self._grace_period.cancel()
            self._grace_period = None
        f = asyncio.ensure_future(writer.read_replay(self), loop=self.loop)
        f.add_done_callback(lambda f: self._end_writer(f, writer))
        self._write_futures.add(f)

    def remove_writer(self, writer):
        self._writers.remove(writer)
        if not self.writers:
            if self._grace_period is not None:
                self._grace_period.cancel()
            self._grace_period = asyncio.ensure_future(
                self._no_writers_grace_period(), loop=self.loop)

    def _end_writer(self, future, writer):
        self._write_futures.remove(future)
        self.remove_writer(writer)

    async def _no_writers_grace_period(self):
        await asyncio.sleep(self.GRACE_PERIOD, loop=self.loop)
        if not self._writers:
            self._ended.set()

    def close(self):
        for f in self._write_futures:
            f.cancel()
        for w in self._writers:
            w.close()
        self._ended.set()

    async def feed(self, offset, data):
        assert(self.position >= offset)
        data_end = offset + len(data)
        if self.position >= data_end:
            return
        start = self.position - data_end
        self.data.write(data[start:])

    def get_data(self, start, end):
        self.data.seek(start)
        return self.data.read(end - start)

    async def wait_for_ended(self):
        await self._ended.wait()
