import asyncio
from asyncio.locks import Event
from replayserver.replaytimestamp import ReplayTimestamp
from replayserver.errors import StreamEndedError


class ReplaySender:
    DELAY = 300

    def __init__(self, stream):
        self._stream = stream
        self._readers = set()
        self._read_futures = set()
        self._ended = Event()
        self.timestamp = ReplayTimestamp(stream, self.delay)

    @property
    def ended(self):
        return self._ended.is_set()

    def add_reader(self, reader):
        if self._stream.ended:
            raise StreamEndedError("Tried to add a reader to an ended stream!")
        self._readers.add(reader)
        f = asyncio.ensure_future(self.write_to_reader(reader))
        f.add_done_callback(lambda f: self._end_reader(f, reader))
        self._read_futures.add(f)

    def remove_reader(self, reader):
        self._readers.remove(reader)
        if not self._readers and self._stream.ended:
            self._ended.set()

    def _end_reader(self, future, reader):
        self._read_futures.remove(future)
        self.remove_reader(reader)

    async def write_to_reader(self, reader):
        stamp = 0
        position = 0
        while True:
            stamp = await self.timestamp.next_stamp(stamp)
            if stamp is None:
                return
            new_position = self.timestamp.position(stamp)
            data = self._stream.get_data(position, new_position)
            position = new_position
            await reader.write(data)

    def close(self):
        for f in self._read_futures:
            f.cancel()
        for r in self._readers:
            r.close()
        self._ended.set()

    async def wait_for_ended(self):
        await self._ended.wait()
