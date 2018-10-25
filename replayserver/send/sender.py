import asyncio
from asyncio.locks import Event
from contextlib import contextmanager

from replayserver.send.stream import DelayedReplayStream
from replayserver.errors import MalformedDataError
from replayserver.collections import AsyncCounter


class Sender:
    def __init__(self, delayed_stream):
        self._stream = delayed_stream
        self._conn_count = AsyncCounter()
        self._ended = Event()
        asyncio.ensure_future(self._lifetime())

    @classmethod
    def build(cls, stream, **kwargs):
        delayed_stream = DelayedReplayStream.build(stream, **kwargs)
        return cls(delayed_stream)

    @contextmanager
    def _connection_count(self):
        self._conn_count.inc()
        try:
            yield
        finally:
            self._conn_count.dec()

    async def handle_connection(self, connection):
        with self._connection_count():
            await self._write_header(connection)
            await self._write_replay(connection)

    async def _write_header(self, connection):
        header = await self._stream.wait_for_header()
        if header is None:
            raise MalformedDataError("Malformed replay header")
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

    def close(self):
        pass

    async def _lifetime(self):
        await self._stream.wait_for_ended()
        await self._conn_count.wait_until_empty()
        self._ended.set()

    async def wait_for_ended(self):
        await self._ended.wait()
