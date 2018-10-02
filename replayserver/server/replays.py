import asyncio
from asyncio.locks import Event
from collections.abc import MutableMapping

from replayserver.server.replay import Replay
from replayserver.server.connection import ConnectionHeader
from replayserver.errors import CannotAcceptConnectionError


class AsyncDict(MutableMapping):
    def __init__(self):
        self._dict = {}
        self._empty = Event()

    def __getitem__(self, key):
        return self._dict[key]

    def __setitem__(self, key, value):
        self._empty.clear()
        self._dict[key] = value

    def __delitem__(self, key):
        del self._dict[key]
        if not self._dict:
            self._empty.set()

    def __iter__(self):
        return iter(self._dict)

    def __len__(self):
        return len(self._dict)

    async def wait_until_empty(self):
        await self._empty.wait()


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

    async def _remove_replay_when_done(self, game_id, replay):
        await replay.wait_for_ended()
        self._replays.pop(game_id, None)

    async def stop_all(self):
        self._closing = True
        replays = self._replays.values()
        for replay in replays:
            replay.close()
        await self._replays.wait_until_empty()

    # Tiny bit of introspection for easier testing
    def __contains__(self, game_id):
        return game_id in self._replays
