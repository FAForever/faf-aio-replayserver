from replayserver.errors import BookkeepingError
from replayserver.bookkeeping.storage import ReplaySaver
from replayserver.bookkeeping.database import ReplayDatabaseQueries


class Bookkeeper:
    def __init__(self, queries, saver):
        self._queries = queries
        self._saver = saver

    @classmethod
    def build(cls, database, storage, **config):
        queries = ReplayDatabaseQueries(database)
        saver = ReplaySaver(storage, queries)
        return cls(queries, saver)

    async def save_replay(self, game_id, stream):
        try:
            await self._saver.save_replay(game_id, stream)
        except BookkeepingError:
            pass    # TODO - log
