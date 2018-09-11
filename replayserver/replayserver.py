import asyncio
from replayserver.replayconnection import ReplayConnection
from replayserver.replay import Replay
from replayserver.errors import StreamEndedError


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
        return not (connection.type == ReplayConnection.Type.READER
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


class ReplayServer:
    def __init__(self, server, replays, replay_connection_builder):
        self._server = server
        self._replays = replays
        self._replay_connection_builder = replay_connection_builder
        self._connections = set()

    @classmethod
    async def build(cls, port):
        def server(cb):
            return asyncio.streams.start_server(cb, port=port)
        replays = Replays.build()
        return cls(server, replays, ReplayConnection)

    async def start(self):
        # A tiny hack - we can't pass in a server directly since we have to
        # register our method as a callback at creation
        self._server = await self._server(self.handle_connection)

    async def stop(self):
        self._server.close()
        await self._server.wait_closed()
        for connection in self._connections:
            connection.close()
        await self._replays.stop()

    async def handle_connection(self, reader, writer):
        connection = self._replay_connection_builder(reader, writer)
        self._connections.add(connection)
        try:
            try:
                await connection.read_header()
            except ValueError as e:
                raise ConnectionError from e
            await self._replays.handle_connection(connection)
        except (ConnectionError, StreamEndedError):
            pass    # TODO - log
        finally:
            await connection.close()
            self._connections.remove(connection)
