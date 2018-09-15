import asyncio
from asyncio.locks import Event
from collections.abc import MutableMapping

from replayserver.server.replay import Replay
from replayserver.server.connection import Connection
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
    def build(cls):
        return cls(Replay.build)

    async def handle_connection(self, connection):
        if not self._can_add_to_replay(connection):
            raise CannotAcceptConnectionError(
                "Cannot add connection to a replay")    # FIXME - details
        if connection.uid not in self._replays:
            self._create(connection.uid)
        replay = self._replays[connection.uid]
        await replay.handle_connection(connection)

    def _can_add_to_replay(self, connection):
        if self._closing:
            return False
        if (connection.type == Connection.Type.READER
                and connection.uid not in self._replays):
            return False
        return True

    def _create(self, uid):
        if uid in self._replays:
            return
        replay = self._replay_builder()
        self._replays[uid] = replay
        asyncio.ensure_future(self._remove_replay_when_done(uid, replay))

    async def _remove_replay_when_done(self, uid, replay):
        await replay.wait_for_ended()
        self._replays.pop(uid, None)

    async def stop(self):
        self._closing = True
        replays = self._replays.values()
        for replay in replays:
            replay.close()
        await self._replays.wait_until_empty()

    # Tiny bit of introspection for easier testing
    def __contains__(self, uid):
        return uid in self._replays
