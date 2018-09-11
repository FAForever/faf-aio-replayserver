from asyncio.locks import Event
from replayserver.send.stream import DelayedReplayStream
from replayserver.errors import StreamEndedError


class ReplaySender:
    DELAY = 300

    def __init__(self, stream):
        self._stream = DelayedReplayStream(stream)
        self._connections = set()
        self._ended = Event()
        self._closed = False

    def _add_connection(self, connection):
        self._connections.add(connection)

    def _remove_connection(self, connection):
        self._connections.remove(connection)
        if not self._connections and self._stream.is_finished():
            self._ended.set()

    def accepts_connections(self):
        return not self._stream.is_finished() and not self._closed

    async def handle_connection(self, connection):
        if not self.accepts_connections():
            raise StreamEndedError  # FIXME
        self._add_connection(connection)
        try:
            await self._write_header(connection)
            await self._write_replay(connection)
        except StreamEndedError:
            raise
        finally:
            self._remove_connection(connection)

    async def _write_header(self, connection):
        try:
            header = await self._stream.read_header()
            connection.write(header)   # TODO
        except ValueError as e:
            raise StreamEndedError from e

    async def _write_replay(self, connection):
        position = 0
        while not self._closed:
            data = await self._stream.read_data(position)
            if not data:
                break
            position += len(data)
            connection.write(data)

    def close(self):
        # This will prevent new connections and stop existing ones quickly.
        self._closed = True

    async def wait_for_ended(self):
        await self._ended.wait()
