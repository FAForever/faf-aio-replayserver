import asyncio
from asyncio.locks import Event
from contextlib import contextmanager

from replayserver.server.connection import ConnectionHeader
from replayserver.send.sender import Sender
from replayserver.receive.merger import Merger
from replayserver.errors import MalformedDataError
from replayserver.logging import logger


class Replay:
    def __init__(self, merger, sender, bookkeeper, timeout, game_id):
        self.merger = merger
        self.sender = sender
        self.bookkeeper = bookkeeper
        self._game_id = game_id
        self._connections = set()
        self._timeout = timeout
        self._ended = Event()
        asyncio.ensure_future(self._lifetime())
        self._force_close = asyncio.ensure_future(self._timeout_force_close())

    @classmethod
    def build(cls, game_id, bookkeeper, *, config_replay_forced_end_time,
              **kwargs):
        merger = Merger.build(**kwargs)
        sender = Sender.build(merger.canonical_stream, **kwargs)
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
        with self._track_connection(connection):
            logger.debug(f"{self} - new connection, {connection}")
            if header.type == ConnectionHeader.Type.WRITER:
                await self.merger.handle_connection(connection)
            elif header.type == ConnectionHeader.Type.READER:
                await self.sender.handle_connection(connection)
            else:
                raise MalformedDataError("Invalid connection type")
            logger.debug(f"{self} - connection over, {connection}")

    def close(self):
        self.merger.close()
        self.sender.close()
        for connection in self._connections:
            connection.close()

    async def _timeout_force_close(self):
        try:
            await asyncio.wait_for(self.wait_for_ended(),
                                   timeout=self._timeout)
        except asyncio.TimeoutError:
            logger.info(f"Timeout - force-ending {self}")
            self.close()

    async def _lifetime(self):
        await self.merger.wait_for_ended()
        logger.debug(f"{self} write phase ended")
        await self.bookkeeper.save_replay(self._game_id,
                                          self.merger.canonical_stream)
        await self.sender.wait_for_ended()
        self._force_close.cancel()
        self._ended.set()
        logger.debug(f"Lifetime of {self} ended")

    async def wait_for_ended(self):
        await self._ended.wait()

    def __str__(self):
        return f"Replay {self._game_id}"
