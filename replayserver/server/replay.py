import asyncio
from replayserver.server.connection import Connection
from replayserver.send.sender import Sender
from replayserver.receive.merger import Merger
from replayserver.errors import MalformedDataError


class Replay:
    REPLAY_TIMEOUT = 60 * 60 * 5

    def __init__(self, merger, sender):
        self.merger = merger
        self.sender = sender
        self._timeout = asyncio.ensure_future(self._wait_until_timeout())
        self._timeout.add_done_callback(lambda _: self._perform_timeout())

    @classmethod
    def build(cls):
        merger = Merger.build()
        sender = Sender(merger.canonical_replay)
        return cls(merger, sender)

    async def handle_connection(self, connection):
        if connection.type == Connection.Type.READER:
            await self.merger.handle_connection(connection)
        elif connection.type == Connection.Type.WRITER:
            await self.stream.handle_connection(connection)
        else:
            raise MalformedDataError("Invalid connection type")

    async def _wait_until_timeout(self):
        await asyncio.sleep(self.REPLAY_TIMEOUT)

    def close(self):
        if self._timeout is not None:
            self._timeout.cancel()
            self._timeout = None
        self.merger.close()
        self.sender.close()

    def do_not_wait_for_more_connections(self):
        self.merger.do_not_wait_for_more_connections()

    def _perform_timeout(self):
        self.do_not_wait_for_more_connections()
        self.close()

    async def wait_for_ended(self):
        await self.stream.wait_for_ended()
        await self.sender.wait_for_ended()
