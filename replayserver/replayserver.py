import asyncio
from replayserver.replayconnection import ReplayConnection
from replayserver.replay import Replay
from replayserver.errors import StreamEndedError


class Replays:
    def __init__(self):
        self._replays = {}

    def get_matching_replay(self, connection):
        if (connection.type == ReplayConnection.Type.READER
                and connection.uid not in self._replays):
            raise ValueError("Can't read a nonexisting stream!")
        if connection.uid not in self._replays:
            self._create(connection.uid)
        return self._replays[connection.uid]

    def _create(self, uid):
        if uid in self._replays:
            return
        replay = Replay()
        self._replays[uid] = replay
        asyncio.ensure_future(self._remove_replay_when_done(uid, replay))

    async def _remove_replay_when_done(self, uid, replay):
        await replay.wait_for_ended()
        self._replays.pop(uid, None)

    async def clear(self):
        replays = self._replays.items()
        for replay in replays:
            replay.close()
        for replay in replays:
            await replay.wait_for_ended()
        self._replays.clear()


class ReplayServer:
    def __init__(self, port):
        self._replays = Replays()
        self._port = port
        self._server = None

    async def start(self):
        self._server = await asyncio.streams.start_server(
            self.handle_connection, port=self._port)

    async def stop(self):
        self._server.close()
        await self._server.wait_closed()
        await self._replays.clear()

    async def handle_connection(self, reader, writer):
        connection = ReplayConnection(reader, writer)
        try:
            await connection.read_header()
            try:
                replay = self._replays.get_matching_replay(connection)
            except ValueError as e:
                raise ConnectionError from e
            replay.add_connection(connection)
        except (ConnectionError, StreamEndedError):
            # TODO - log
            await connection.close()
            return
