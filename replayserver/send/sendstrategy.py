from replayserver.send.stream import DelayedReplayStream
from replayserver.errors import MalformedDataError


class SendStrategy:
    """
    Describes *how* a replay stream is sent to readers, separately from actual
    tracking of readers for a given replay. In the future we might implement
    alternative ways of sending data to replays (e.g. delaying by game-time
    rather than realtime).

    TODO: The code below probably works for anything that implements the
    ReplayStream interface - if we ever have multiple strategies, reuse it.
    """
    def __init__(self, delayed_stream):
        self._stream = delayed_stream

    @classmethod
    def build(cls, canonical_stream, config):
        delayed_stream = DelayedReplayStream.build(canonical_stream, config)
        return cls(delayed_stream)

    async def send_to(self, connection):
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
