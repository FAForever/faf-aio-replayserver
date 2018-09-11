import asyncio
from replayserver.server.replay import Replay
from replayserver.server.connection import Connection


class Replays:
    def __init__(self, replay_builder):
        self._replays = {}
        self._replay_builder = replay_builder
        self._closing = False

    @classmethod
    def build(cls):
        return cls(Replay.build)

    async def handle_connection(self, connection):
        if not self._can_add_to_replay(connection):
            return
        if connection.uid not in self._replays:
            self._create(connection.uid)
        replay = self._replays[connection.uid]
        await replay.handle_connection(connection)

    def _can_add_to_replay(self, connection):
        return not (connection.type == Connection.Type.READER
                    and connection.uid not in self._replays
                    and not self._closing)

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
            replay.do_not_wait_for_more_connections()
        for replay in replays:
            await replay.wait_for_ended()
