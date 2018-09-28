import asyncio
from asyncio.locks import Event
from contextlib import contextmanager
from enum import Enum

from replayserver.server.connection import Connection
from replayserver.send.sender import Sender
from replayserver.receive.merger import Merger
from replayserver.bookkeeping.bookkeeper import Bookkeeper
from replayserver.errors import MalformedDataError, \
    CannotAcceptConnectionError


class ReplayTimeout:
    def __init__(self):
        self._timeout = None

    def set(self, timeout, cb):
        if self._timeout is not None:
            self.cancel()
        self._timeout = asyncio.ensure_future(self._wait(timeout))
        self._timeout.add_done_callback(
            lambda f: cb() if not f.cancelled() else None)

    async def _wait(self, timeout):
        await asyncio.sleep(timeout)

    def cancel(self):
        if self._timeout is not None:
            self._timeout.cancel()
            self._timeout = None


class Replay:
    def __init__(self, merger, sender, bookkeeper, timeout):
        self.merger = merger
        self.sender = sender
        self.bookkeeper = bookkeeper
        self._connections = set()
        self._accepts_connections = True
        self._timeout = ReplayTimeout()
        self._timeout.set(timeout, self.close)
        asyncio.ensure_future(self._replay_lifetime())
        self._ended = Event()

    @classmethod
    def build(cls, *, config_replay_forced_end_time, **kwargs):
        merger = Merger.build(**kwargs)
        sender = Sender(merger.canonical_stream)
        bookkeeper = Bookkeeper()
        return cls(merger, sender, bookkeeper, config_replay_forced_end_time)

    @contextmanager
    def _track_connection(self, connection):
        self._connections.add(connection)
        try:
            yield
        finally:
            self._connections.remove(connection)

    async def handle_connection(self, connection):
        if not self._accepts_connections:
            raise CannotAcceptConnectionError(
                "Replay does not accept connections anymore")
        with self._track_connection(connection):
            if connection.type == Connection.Type.WRITER:
                await self.merger.handle_connection(connection)
            elif connection.type == Connection.Type.READER:
                await self.sender.handle_connection(connection)
            else:
                raise MalformedDataError("Invalid connection type")

    def close(self):
        self._accepts_connections = False
        self._timeout.cancel()
        self.merger.close()
        self.sender.close()
        for connection in self._connections:
            connection.close()

    async def _replay_lifetime(self):
        await self.merger.wait_for_ended()
        self._accepts_connections = False
        await self.bookkeeper.save_replay()
        await self.sender.wait_for_ended()
        self._timeout.cancel()
        self._ended.set()

    async def wait_for_ended(self):
        await self._ended.wait()
