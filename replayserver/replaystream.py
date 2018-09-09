from asyncio.locks import Condition


class ReplayStream:
    def __init__(self):
        self.header = None
        self.data = bytearray()   # TODO - don't keep the entire replay
        self._data_or_done = Condition()
        self._finished = False

    async def _get_header(self):
        pass    # TODO

    def put_data(self, data):
        # TODO - get the header first
        self.data += data
        self._data_or_done.notify_all()

    def finish(self):
        self._finished = True
        self._data_or_done.notify_all()

    async def get_data(self, data, position):
        self._wait_for_data(position)
        if position >= len(self.data):
            return bytes()
        return self.data[position:]

    def _wait_for_data(self, position):
        self._data_or_done.lock()
        self._data_or_done.wait_for(self._have_data(position))
        self._data_or_done.release()

    def _have_data(self, position):
        return self._finished or position < len(self.data)


class ReplayStreamReader:
    def __init__(self, connection):
        self.stream = ReplayStream()
        self._connection = connection

    async def read_all(self):
        while True:
            data = self._connection.read(4096)
            if not data:
                break
            self.stream.put_data(data)
        self.stream.finish()

    def close(self):
        self._connection.close()
