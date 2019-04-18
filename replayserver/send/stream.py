import asyncio
from replayserver.stream import ReplayStream
from replayserver.send.timestamp import Timestamp


class ReplayStreamWriter:
    def __init__(self, stream):
        self._stream = stream

    @classmethod
    def build(cls, stream):
        return cls(stream)

    async def send_to(self, connection):
        await self._write_header(connection)
        if self._stream.header is None:
            return
        await self._write_replay(connection)

    async def _write_header(self, connection):
        header = await self._stream.wait_for_header()
        if header is not None:
            await connection.write(header.data)

    async def _write_replay(self, connection):
        position = 0
        while True:
            data = await self._stream.wait_for_data(position)
            if not data:
                break
            position += len(data)
            conn_open = await connection.write(data)
            if not conn_open:
                break


class DelayedReplayStream(ReplayStream):
    def __init__(self, stream, timestamp):
        ReplayStream.__init__(self)
        self._stream = stream
        self._timestamp = timestamp
        self._current_position = 0
        asyncio.ensure_future(self._track_delayed_stream())

    @classmethod
    def build(cls, stream, config):
        timestamp = Timestamp(stream,
                              config.update_interval,
                              config.replay_delay)
        return cls(stream, timestamp)

    @property
    def header(self):
        return self._stream.header

    def _data_length(self):
        return min(len(self._stream.data), self._current_position)

    def _data_slice(self, s):
        if s.stop is None:
            new_stop = self._current_position
        else:
            new_stop = min(s.stop, self._current_position)
        return self._stream.data[slice(s.start, new_stop, s.step)]

    def _data_bytes(self):
        return self._stream.data[:self._current_position]

    async def _track_delayed_stream(self):
        await self._stream.wait_for_header()
        self._header_available()
        await self._track_data()
        self._end()

    async def _track_data(self):
        async for position in self._timestamp.timestamps():
            if position <= self._current_position:
                continue
            self._current_position = position
            self._data_available()
