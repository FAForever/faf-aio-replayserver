import asyncio


class ReplayStream:
    def __init__(self, connection, sink):
        self.connection = connection
        self.sink = sink
        self._read_future = None
        self.header = None
        self.data = bytearray()   # TODO - don't keep the entire replay

    @property
    def position(self):
        return len(self.data)

    async def _read_forever(self):
        await self._read_header()
        while True:
            chunk = await self.connection.read(4096)
            if not data:
                break
            self.data += chunk
            await self.sink.on_new_data(self)
        self.sink.stream_ended(self)

    async def _read_header(self):
        pass    # TODO

    def start_read_future(self):
        self._read_future = asyncio.ensure_future(self._read_forever())

    def close(self):
        self._read_future.close()
        self.connection.close()
