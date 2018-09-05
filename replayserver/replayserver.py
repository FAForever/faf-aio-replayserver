import asyncio
from collections.ABC import Mapping

from replayserver.replayconnection import ReplayConnection
from replayserver.replay import Replay
from replayserver.errors import StreamEndedError


class Replays(Mapping):
    REPLAY_TIMEOUT = 60 * 60 * 5

    def __init__(self):
        self._replays = {}
        self._replay_waiters = {}

    def __getitem__(self, uid):
        return self._replays[uid]

    def __iter__(self):
        return iter(self._replays)

    def __len__(self):
        return len(self._replays)

    def get_matching(self, uid, connection_type):
        if (connection_type == ReplayConnection.Type.READER
                and uid not in self._replays):
            raise ValueError("Can't read a nonexisting stream!")
        if uid not in self._replays:
            self._create(uid)
        return self[uid]

    def _create(self, uid):
        if uid in self._replays:
            return
        replay = Replay()
        self._replays[uid] = replay
        future = asyncio.ensure_future(self._wait_for_replay(replay))
        future.add_done_callback(lambda f: self._end_replay(f, replay, uid))
        self._replay_waiters.add(future)

    async def _wait_for_replay(self, replay):
        await asyncio.wait_for(replay.wait_for_ended, self.REPLAY_TIMEOUT)

    def _end_replay(self, future, replay, uid):
        self._replay_waiters.remove(future)
        replay.close()
        del self._replays[uid]

    def clear(self):
        for fut in self._replay_waiters:
            fut.cancel()
        self._replay_waiters.clear()
        for replay in list(self._replays.items()):
            replay.close()
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
        self._replays.clear()
        self._server.close()
        await self._server.wait_closed()

    async def handle_connection(self, reader, writer):
        connection = ReplayConnection(reader, writer)
        try:
            type_ = await connection.determine_type()
            uid, replay_name = await connection.get_replay_name()
            try:
                replay = self._replays.get_matching(uid, type_)
            except ValueError as e:
                raise ConnectionError from e
            if type_ == ReplayConnection.Type.READER:
                replay.sender.add_reader(connection)
            else:
                replay.stream.add_writer(connection)
        except (ConnectionError, StreamEndedError):
            # TODO - log
            await connection.close()
            return
