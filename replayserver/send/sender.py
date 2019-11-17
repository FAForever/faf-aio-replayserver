from replayserver.common import ServesConnections
from replayserver.logging import logger


class ReplayStreamWriter:
    def __init__(self, stream, game_id=1):
        self._stream = stream
        self._game_id = game_id

    @classmethod
    def build(cls, stream, game_id=1):
        return cls(stream, game_id)

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
        last_pos = 0
        try:
            while True:
                dlen = await self._stream.wait_for_data(position)
                if dlen == 0:
                    break
                data = self._stream.data[position:position+dlen]
                last_pos = position
                position += dlen
                conn_open = await connection.write(data)
                if not conn_open:
                    break
        finally:
            logger.info((f"Writer for game {self._game_id}, connection {id(connection)} tried to send "
                         f"{position} bytes total, last chunk was {position - last_pos} bytes "
                         f"(plus {len(self._stream.header.data)} bytes)"))


class Sender(ServesConnections):
    def __init__(self, stream, writer):
        ServesConnections.__init__(self)
        self._stream = stream
        self._writer = writer

    @classmethod
    def build(cls, stream, game_id=1):
        writer = ReplayStreamWriter.build(stream, game_id)
        return cls(stream, writer)

    async def _handle_connection(self, connection):
        await self._writer.send_to(connection)

    async def _after_connections_end(self):
        await self._stream.wait_for_ended()

    def __str__(self):
        return "Sender"
