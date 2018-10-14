from replayserver.errors import BookkeepingError
from replayserver.bookkeeping.storage import ReplaySaver
from replayserver.bookkeeping.database import ReplayDatabaseQueries
from replayserver.logging import logger


class Bookkeeper:
    def __init__(self, queries, saver):
        self._queries = queries
        self._saver = saver

    @classmethod
    def build(cls, database, **config):
        queries = ReplayDatabaseQueries(database)
        saver = ReplaySaver.build(queries, **config)
        return cls(queries, saver)

    async def save_replay(self, game_id, stream):
        try:
            logger.debug(f"Saving replay {game_id}")
            await self._saver.save_replay(game_id, stream)
            logger.debug(f"Saved replay {game_id}")
        except BookkeepingError as e:
            logger.error(f"Failed to save replay for game {game_id}: {str(e)}")
