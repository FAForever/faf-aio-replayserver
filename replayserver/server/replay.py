import asyncio
from asyncio.locks import Event
from contextlib import contextmanager

from replayserver.server.connection import ConnectionHeader
from replayserver.send.sender import Sender
from replayserver.receive.merger import Merger, MergerConfig, DelayConfig
from replayserver.receive.offlinemerger import OfflineReplayMerger
from replayserver.errors import MalformedDataError
from replayserver.logging import logger
from replayserver import config


class ReplayConfig(config.Config):
    _options = {
        "forced_end_time": {
            "parser": config.positive_float,
            "doc": "Time in seconds after which a replay is forcefully ended."
        },
        "grace_period": {
            "parser": config.nonnegative_float,
            "doc": ("Time in seconds after which a replay with no writers "
                    "will consider itself over.")
        },
    }

    def __init__(self, config):
        super().__init__(config)
        self.merge = MergerConfig(config.with_namespace("merge"))
        self.delay = DelayConfig(config.with_namespace("delay"))


class Replay:
    def __init__(self, merger, sender, offline_merger, bookkeeper, config,
                 game_id):
        self.merger = merger
        self.sender = sender
        self.offline_merger = offline_merger
        self.bookkeeper = bookkeeper
        self._game_id = game_id
        self._connections = set()
        self._ended = Event()
        self._lifetime_coroutines = [
            asyncio.ensure_future(self._force_closing(config.forced_end_time)),
            asyncio.ensure_future(self._write_phase(config.grace_period))
        ]
        asyncio.ensure_future(self._lifetime())

    @classmethod
    def build(cls, game_id, bookkeeper, config):
        merger = Merger.build(config.merge, config.delay)
        sender = Sender.build(merger.canonical_stream)
        offline_merger = OfflineReplayMerger.build()
        return cls(merger, sender, offline_merger, bookkeeper, config, game_id)

    @contextmanager
    def _track_connection(self, connection):
        logger.debug(f"{self} - new connection, {connection}")
        self._connections.add(connection)
        try:
            yield
        finally:
            self._connections.remove(connection)
            logger.debug(f"{self} - connection over, {connection}")

    async def handle_connection(self, header, connection):
        with self._track_connection(connection):
            if header.type == ConnectionHeader.Type.WRITER:
                replay = await self.merger.handle_connection(connection)
                self.offline_merger.add_replay(replay)
            elif header.type == ConnectionHeader.Type.READER:
                await self.sender.handle_connection(connection)
            else:
                raise MalformedDataError("Invalid connection type")

    def close(self):
        self.merger.stop_accepting_connections()
        self.sender.stop_accepting_connections()
        for connection in self._connections:
            connection.close()

    async def _force_closing(self, timeout):
        await asyncio.sleep(timeout)
        logger.info(f"Timeout - force-ending {self}")
        self.close()

    async def _write_phase(self, grace_period):
        await self.merger.no_connections_for(grace_period)
        self.merger.stop_accepting_connections()
        self.sender.stop_accepting_connections()

    async def _save_replay(self):
        best_replay = self.offline_merger.get_best_replay()
        if best_replay is None:
            logger.info(f"No writers for {self} had data, not saving replay")
        else:
            await self.bookkeeper.save_replay(self._game_id, best_replay)

    async def _lifetime(self):
        await self.merger.wait_for_ended()
        logger.debug(f"{self} write phase ended")
        await self._save_replay()
        await self.sender.wait_for_ended()
        for coro in self._lifetime_coroutines:
            coro.cancel()
        self._ended.set()
        logger.debug(f"Lifetime of {self} ended")

    async def wait_for_ended(self):
        await self._ended.wait()

    def __str__(self):
        return f"Replay {self._game_id}"
