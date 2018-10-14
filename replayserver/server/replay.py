import asyncio
from asyncio.locks import Event
from contextlib import contextmanager

from replayserver.server.connection import ConnectionHeader
from replayserver.send.sender import Sender
from replayserver.receive.merger import Merger
from replayserver.errors import MalformedDataError, \
    CannotAcceptConnectionError
from replayserver.logging import logger


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
    def __init__(self, merger, sender, bookkeeper, timeout, game_id):
        self.merger = merger
        self.sender = sender
        self.bookkeeper = bookkeeper
        self._game_id = game_id
        self._connections = set()
        self._accepts_connections = True
        self._timeout = ReplayTimeout()
        self._timeout.set(timeout, self.force_close)
        self._ended = Event()
        asyncio.ensure_future(self._replay_lifetime())

    @classmethod
    def build(cls, game_id, bookkeeper, *, config_replay_forced_end_time,
              **kwargs):
        merger = Merger.build(**kwargs)
        sender = Sender(merger.canonical_stream)
        return cls(merger, sender, bookkeeper, config_replay_forced_end_time,
                   game_id)

    @contextmanager
    def _track_connection(self, connection):
        self._connections.add(connection)
        try:
            yield
        finally:
            self._connections.remove(connection)

    async def handle_connection(self, header, connection):
        if not self._accepts_connections:
            raise CannotAcceptConnectionError(
                "Replay does not accept connections anymore")
        with self._track_connection(connection):
            if header.type == ConnectionHeader.Type.WRITER:
                logger.debug(f"{self} - new writer connection")
                await self.merger.handle_connection(connection)
                logger.debug(f"{self} - writer connection done")
            elif header.type == ConnectionHeader.Type.READER:
                logger.debug(f"{self} - new reader connection")
                await self.sender.handle_connection(connection)
                logger.debug(f"{self} - reader connection done")
            else:
                raise MalformedDataError("Invalid connection type")

    def close(self):
        self._accepts_connections = False
        self._timeout.cancel()
        self.merger.close()
        self.sender.close()
        for connection in self._connections:
            connection.close()

    def force_close(self):
        logger.info(f"Timeout - force-ending {self}")
        self.close()

    async def _replay_lifetime(self):
        await self.merger.wait_for_ended()
        logger.debug(f"{self} write phase ended")
        self._accepts_connections = False
        await self.bookkeeper.save_replay(self._game_id,
                                          self.merger.canonical_stream)
        await self.sender.wait_for_ended()
        self._timeout.cancel()
        self._ended.set()
        logger.debug(f"Lifetime of {self} ended")

    async def wait_for_ended(self):
        await self._ended.wait()

    def __str__(self):
        return f"Replay {self._game_id}"
