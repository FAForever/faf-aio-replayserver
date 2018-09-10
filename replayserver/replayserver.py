import asyncio
from replayserver.replayconnection import ReplayConnection
from replayserver.replay import Replay
from replayserver.errors import StreamEndedError


class Replays:
    def __init__(self):
        self._replays = {}

    async def handle_connection(self, connection):
        if not self._can_add_to_replay(connection):
            return
        if connection.uid not in self._replays:
            self._create(connection.uid)
        replay = self._replays[connection.uid]
        await replay.handle_connection(connection)

    def _can_add_to_replay(self, connection):
        return not (connection.type == ReplayConnection.Type.READER
                    and connection.uid not in self._replays)

    def _create(self, uid):
        if uid in self._replays:
            return
        replay = Replay.build()
        self._replays[uid] = replay
        asyncio.ensure_future(self._remove_replay_when_done(uid, replay))

    async def _remove_replay_when_done(self, uid, replay):
        await replay.wait_for_ended()
        self._replays.pop(uid, None)


class ReplayServer:
    def __init__(self, port):
        self._replays = Replays()
        self._connections = set()
        self._port = port
        self._server = None

    async def start(self):
        self._server = await asyncio.streams.start_server(
            self.handle_connection, port=self._port)

    async def stop(self):
        self._server.close()
        await self._server.wait_closed()
        for connection in self._connections:
            connection.close()
        for replay in self._replays:
            replay.do_not_wait_for_more_connections()
        for replay in self._replays:
            await replay.wait_for_ended()

    async def handle_connection(self, reader, writer):
        connection = ReplayConnection(reader, writer)
        self._connections.add(connection)
        try:
            await connection.read_header()
            try:
                replay = self._replays.get_matching_replay(connection)
            except ValueError as e:
                raise ConnectionError from e
            replay.handle_connection(connection)
        except (ConnectionError, StreamEndedError):
            pass    # TODO - log
        finally:
            await connection.close()
            self._connections.remove(connection)
