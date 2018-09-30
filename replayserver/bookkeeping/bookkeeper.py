from replayserver.errors import BookkeepingError
from replayserver.bookkeeping.storage import ReplayStorage, ReplaySaver
from replayserver.bookkeeping.database import Database, ReplayDatabaseQueries


class Bookkeeper:
    def __init__(self, database, queries, storage, saver):
        self._database = database
        self._queries = queries
        self._storage = storage
        self._saver = saver

    @classmethod
    def build(cls, **config):
        database = Database.build(**config)
        queries = ReplayDatabaseQueries(database)
        storage = ReplayStorage.build(**config)
        saver = ReplaySaver(storage, queries)
        return cls(database, queries, storage, saver)

    async def start(self):
        await self._database.start()

    async def save_replay(self, game_id, stream):
        try:
            await self._saver.save_replay(game_id, stream)
            await self._database.register_replay(game_id)
        except BookkeepingError:
            pass    # TODO - log
