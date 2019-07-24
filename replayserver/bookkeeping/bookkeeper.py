from replayserver import config, metrics
from replayserver.bookkeeping.analyzer import ReplayAnalyzer
from replayserver.bookkeeping.database import ReplayDatabaseQueries
from replayserver.bookkeeping.storage import ReplaySaver
from replayserver.errors import BookkeepingError
from replayserver.logging import logger


class BookkeeperConfig(config.Config):
    _options = {
        "vault_path": {
            "doc": "Root directory for saved replays.",
            "parser": config.is_dir
        },
    }


class Bookkeeper:
    def __init__(self, queries, saver, analyzer):
        self._queries = queries
        self._saver = saver
        self._analyzer = analyzer

    @classmethod
    def build(cls, database, config):
        queries = ReplayDatabaseQueries(database)
        saver = ReplaySaver.build(queries, config)
        analyzer = ReplayAnalyzer()
        return cls(queries, saver, analyzer)

    async def save_replay(self, game_id, stream):
        try:
            logger.debug(f"Saving replay {game_id}")
            await self._saver.save_replay(game_id, stream)
            logger.debug(f"Saved replay {game_id}")
            metrics.saved_replays.inc()
        except BookkeepingError as e:
            logger.warning(f"Failed to save replay for game {game_id}: {e}")

        try:
            logger.debug(f"Analyzing replay {game_id}")
            ticks = self._analyzer.get_replay_ticks(stream.data.bytes())
            logger.debug(f"Updating tick count for game {game_id}")
            await self._queries.update_game_stats(game_id, ticks)
        except BookkeepingError as e:
            logger.warning(f"Failed to analyze replay for game {game_id}: {e}")
