from replayserver.common import ServesConnections


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


class Sender(ServesConnections):
    def __init__(self, stream, writer):
        ServesConnections.__init__(self)
        self._stream = stream
        self._writer = writer

    @classmethod
    def build(cls, stream):
        writer = ReplayStreamWriter.build(stream)
        return cls(stream, writer)

    async def _handle_connection(self, connection):
        await self._writer.send_to(connection)

    async def _after_connections_end(self):
        await self._stream.wait_for_ended()

    def __str__(self):
        return "Sender"
