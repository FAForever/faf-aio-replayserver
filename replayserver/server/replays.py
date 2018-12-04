import asyncio

from replayserver import metrics
from replayserver.collections import AsyncDict
from replayserver.server.replay import Replay
from replayserver.server.connection import ConnectionHeader
from replayserver.errors import CannotAcceptConnectionError
from replayserver.logging import logger


class Replays:
    def __init__(self, replay_builder):
        self._replays = AsyncDict()
        self._replay_builder = replay_builder
        self._closing = False

    @classmethod
    def build(cls, bookkeeper, **kwargs):
        return cls(lambda game_id: Replay.build(game_id, bookkeeper, **kwargs))

    async def handle_connection(self, header, connection):
        replay = self._get_matching_replay(header)
        await replay.handle_connection(header, connection)

    def _get_matching_replay(self, header):
        if not self._can_add_to_replay(header):
            raise CannotAcceptConnectionError(
                "Cannot add connection to a replay")
        if header.game_id not in self._replays:
            self._create(header.game_id)
        return self._replays[header.game_id]

    def _can_add_to_replay(self, header):
        if self._closing:
            return False
        if (header.type == ConnectionHeader.Type.READER
                and header.game_id not in self._replays):
            return False
        return True

    def _create(self, game_id):
        replay = self._replay_builder(game_id)
        self._replays[game_id] = replay
        asyncio.ensure_future(self._remove_replay_when_done(game_id, replay))
        logger.debug(f"New Replay created: id {game_id}")
        metrics.running_replays.inc()

    async def _remove_replay_when_done(self, game_id, replay):
        await replay.wait_for_ended()
        self._replays.pop(game_id, None)
        logger.debug(f"Replay removed: id {game_id}")
        metrics.running_replays.dec()
        metrics.finished_replays.inc()

    async def stop_all(self):
        logger.info("Stopping all replays")
        self._closing = True
        replays = self._replays.values()
        if replays:
            await asyncio.wait([replay.close() for replay in replays])
        await self._replays.wait_until_empty()

    # Tiny bit of introspection for easier testing
    def __contains__(self, game_id):
        return game_id in self._replays

    async def wait_for_replay(self, game_id):
        return await self._replays.wait_for_key(game_id)
