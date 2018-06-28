import asyncio
from asyncio.locks import Condition, Event
from asyncio.streams import IncompleteReadError, LimitOverrunError
from enum import Enum
from io import BytesIO


class ReplayConnection:
    class Type(Enum):
        READER = 0
        WRITER = 1

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.position = 0

    async def determine_type(self):
        try:
            prefix = await self.reader.readexactly(2)
            if prefix == b"P/":
                return self.Type.WRITER
            elif prefix == b"G/":
                return self.Type.READER
            else:
                raise ConnectionError("Unexpected data received")
        except IncompleteReadError:
            raise ConnectionError("Disconnected before type was determined")

    async def get_replay_name(self):
        try:
            line = await self.reader.readuntil(b'\0')[:-1].encode()
            uid, name = line.split("/", 1)
            return uid, name
        except IncompleteReadError:
            raise ConnectionError("Disconnected before receiving replay data")
        except (LimitOverrunError, ValueError, UnicodeDecodeError):
            raise ConnectionError("Unexpected data received")

    async def read_replay(self, replay):
        while True:
            data = await self.reader.read(4096)
            if not data:
                break
            await replay.feed(self.position, data)
            self.position += len(data)

    async def write_replay(self, data):
        self.writer.write(data)
        await self.writer.drain()

    async def close(self):
        await self.writer.drain()
        self.writer.close()
        self.reader.close()


class StreamEndedError(Exception):
    pass


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


class ReplaySender:
    DELAY = 300

    def __init__(self, stream, loop):
        self._loop = loop
        self._stream = stream
        self._readers = set()
        self._read_futures = set()
        self._ended = Event(loop=self._loop)
        self.timestamp = ReplayTimestamp(stream, loop, self.delay)

    @property
    def ended(self):
        return self._ended.is_set()

    def add_reader(self, reader):
        if self._stream.ended:
            raise StreamEndedError("Tried to add a reader to an ended stream!")
        self._readers.add(reader)
        f = asyncio.ensure_future(self.write_to_reader(reader),
                                  loop=self._loop)
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
            await reader.write_replay(data)

    def close(self):
        for f in self._read_futures:
            f.cancel()
        for r in self._readers:
            r.close()
        self._ended.set()

    async def wait_for_ended(self):
        await self._ended.wait()


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


class ReplayServer:
    REPLAY_TIMEOUT = 60 * 60 * 5

    def __init__(self, port):
        self._replays = {}
        self._replay_waiters = {}
        self._port = port
        self._server = None

    def get_replay(self, uid):
        if uid not in self._replays:
            replay = Replay()
            self._replays[uid] = replay
            future = asyncio.ensure_future(self._wait_for_replay(replay))
            future.add_done_callback(
                lambda f: self._end_replay(f, replay, uid))
            self._replay_waiters.add(future)
        return self._replays[uid]

    async def _wait_for_replay(self, replay):
        await asyncio.wait_for(replay.wait_for_ended, self.REPLAY_TIMEOUT)

    def _end_replay(self, future, replay, uid):
        self._replay_waiters.remove(future)
        replay.close()
        del self._replays[uid]

    async def start(self):
        self._server = await asyncio.streams.start_server(
            self.handle_connection, port=self._port)

    async def stop(self):
        for f in self._replay_waiters:
            f.cancel()
        for r in self._replays.values():
            r.close()
        self._server.close()
        await self._server.wait_closed()

    async def handle_connection(self, reader, writer):
        connection = ReplayConnection(reader, writer)
        try:
            type_ = await connection.determine_type()
            uid, replay_name = await connection.get_replay_name()
            replay = self.get_replay(uid)
            if type_ == ReplayConnection.Type.READER:
                replay.sender.add_reader(connection)
            else:
                replay.stream.add_writer(connection)
        except (ConnectionError, StreamEndedError):
            # TODO - log
            await connection.close()
            return


if __name__ == "__main__":
    PORT = 15000
    server = ReplayServer(PORT)
    loop = asyncio.get_event_loop()
    start = asyncio.ensure_future(server.start())
    try:
        loop.run_forever()
    except Exception:
        loop.run_until_complete(server.stop())
    finally:
        loop.close()
