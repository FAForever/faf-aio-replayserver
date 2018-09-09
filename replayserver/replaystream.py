class ReplayStream:
    """
    Has to be called before read_data.
    """
    async def read_header(self):
        raise NotImplementedError

    async def read(self):
        raise NotImplementedError

    def data_length(self):
        raise NotImplementedError

    def data_from(self, position):
        raise NotImplementedError

    def is_complete(self):
        raise NotImplementedError

    async def read_data(self, position):
        while position >= self.data_length() and not self.is_complete():
            await self.read()
        return self.data_from(position)


class ConnectionReplayStream(ReplayStream):
    def __init__(self, connection):
        ReplayStream.__init__(self)
        self._connection = connection
        self.header = None
        self.data = bytearray()
        self._finished = False

    async def read_header(self):
        pass    # TODO

    async def read(self):
        data = self._connection.read(4096)
        if not data:
            self._finished = True
        self.data += data

    def data_length(self):
        return len(self.data)

    def data_from(self, position):
        return self.data[position:]

    def is_complete(self):
        return self._finished
