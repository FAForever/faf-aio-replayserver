import asyncio
from replayserver.replayconnection import ReplayConnection
from replayserver.replay import Replay
from replayserver.errors import StreamEndedError


class ReplayServer:
    REPLAY_TIMEOUT = 60 * 60 * 5

    def __init__(self, port):
        self._replays = {}
        self._replay_waiters = {}
        self._port = port
        self._server = None

    def get_replay(self, uid):
        if uid not in self._replays:
            replay = Replay()
            self._replays[uid] = replay
            future = asyncio.ensure_future(self._wait_for_replay(replay))
            future.add_done_callback(
                lambda f: self._end_replay(f, replay, uid))
            self._replay_waiters.add(future)
        return self._replays[uid]

    async def _wait_for_replay(self, replay):
        await asyncio.wait_for(replay.wait_for_ended, self.REPLAY_TIMEOUT)

    def _end_replay(self, future, replay, uid):
        self._replay_waiters.remove(future)
        replay.close()
        del self._replays[uid]

    async def start(self):
        self._server = await asyncio.streams.start_server(
            self.handle_connection, port=self._port)

    async def stop(self):
        for f in self._replay_waiters:
            f.cancel()
        for r in self._replays.values():
            r.close()
        self._server.close()
        await self._server.wait_closed()

    async def handle_connection(self, reader, writer):
        connection = ReplayConnection(reader, writer)
        try:
            type_ = await connection.determine_type()
            uid, replay_name = await connection.get_replay_name()
            if (type_ == ReplayConnection.Type.READER
                    and uid not in self._replays):
                raise ConnectionError("Can't read a nonexisting stream!")
            replay = self.get_replay(uid)
            if type_ == ReplayConnection.Type.READER:
                replay.sender.add_reader(connection)
            else:
                replay.stream.add_writer(connection)
        except (ConnectionError, StreamEndedError):
            # TODO - log
            await connection.close()
            return
