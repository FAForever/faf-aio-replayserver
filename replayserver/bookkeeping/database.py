import os
import time
import aiomysql
from aiomysql import create_pool, DatabaseError
from replayserver.errors import BookkeepingError
from replayserver.logging import logger
from replayserver import config


class DatabaseConfig(config.Config):
    _options = {
        "host": {
            "doc": "FAF DB host."
        },
        "port": {
            "doc": "FAF DB port.",
            "parser": config.positive_int
        },
        "user": {
            "doc": "FAF DB username."
        },
        "password": {
            "doc": "FAF DB password."
        },
        "name": {
            "doc": "FAF DB name."
        },
    }


class Database:
    def __init__(self, pool_builder, config):
        self._pool_builder = pool_builder
        self._config = config
        self._connection_pool = None

    @classmethod
    def build(cls, config):
        return cls(create_pool, config)

    async def start(self):
        self._connection_pool = await self._pool_builder(
            host=self._config.host,
            port=self._config.port,
            user=self._config.user,
            password=self._config.password,
            db=self._config.name)
        logger.info("Initialized database connection pool")

    async def execute(self, query, params=[]):
        if self._connection_pool is None:
            raise BookkeepingError("Tried to run query while pool is closed!")
        try:
            async with self._connection_pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute(query, *params)
                    data = await cur.fetchall()
                await conn.commit()
            return data
        except (DatabaseError, RuntimeError) as e:
            raise BookkeepingError(f"Failed to run database query: {str(e)}")

    async def stop(self):
        self._connection_pool.close()
        await self._connection_pool.wait_closed()
        self._connection_pool = None
        logger.info("Closed database connection pool")


class ReplayDatabaseQueries:
    def __init__(self, db):
        self._db = db

    async def get_teams_in_game(self, game_id):
        query = """
            SELECT
                `login`.`login` AS login,
                `game_player_stats`.`team` AS team
            FROM `game_stats`
            INNER JOIN `game_player_stats`
              ON `game_player_stats`.`gameId` = `game_stats`.`id`
            INNER JOIN `login`
              ON `login`.id = `game_player_stats`.`playerId`
            WHERE `game_stats`.`id` = %s AND `game_player_stats`.`AI` = 0
        """
        logger.debug(f"Performing query: {query}")
        players = await self._db.execute(query, (game_id,))
        if not players:
            raise BookkeepingError("No game players found")
        teams = {}
        for player in players:
            teams.setdefault(player['team'], []).append(player['login'])
        return teams

    async def get_game_stats(self, game_id):
        """
        Gets the game information.
        """
        query = """
            SELECT
                `game_stats`.`startTime` AS start_time,
                `game_stats`.`endTime` AS end_time,
                `game_stats`.`gameType` AS game_type,
                `login`.`login` AS host,
                `game_stats`.`gameName` AS game_name,
                `game_featuredMods`.`gamemod` AS game_mod,
                `table_map`.`filename` AS file_name
            FROM `game_stats`
            LEFT JOIN `table_map`
              ON `game_stats`.`mapId` = `table_map`.`id`
            LEFT JOIN `login`
              ON `login`.id = `game_stats`.`host`
            LEFT JOIN  `game_featuredMods`
              ON `game_stats`.`gameMod` = `game_featuredMods`.`id`
            WHERE `game_stats`.`id` = %s
        """
        player_query = """
           SELECT COUNT(*) FROM `game_player_stats`
           WHERE `game_player_stats`.`gameId` = %s
        """
        logger.debug(f"Performing query: {query}")
        game_stats = await self._db.execute(query, (game_id,))
        logger.debug(f"Performing query: {player_query}")
        player_count = await self._db.execute(player_query, (game_id,))
        player_count = player_count[0]['COUNT(*)']
        if not game_stats:
            raise BookkeepingError(f"No stats found for game {game_id}")
        if player_count == 0:
            raise BookkeepingError(f"No players found for game {game_id}")
        start_time = game_stats[0]['start_time'].timestamp()

        # 'mapname' is a filename on the content server containing the map
        mapname = game_stats[0]['file_name']
        if mapname is None:
            # Legacy replay server is forgiving like this. We replicate its
            # behaviour.
            logger.warning(f"Map missing for game {game_id}! Saving anyway.")
            mapname = "None"
        else:
            mapname = os.path.splitext(os.path.basename(mapname))[0]

        # We might end a replay before end_time is set in the db!
        end_time = game_stats[0]['end_time']
        if end_time is None:
            end_time = time.time()
        else:
            end_time = end_time.timestamp()
        return {
            'featured_mod': game_stats[0]['game_mod'],
            'game_type': game_stats[0]['game_type'],
            'recorder': game_stats[0]['host'],
            'host': game_stats[0]['host'],
            'launched_at': start_time,
            'game_end': end_time,
            'title': game_stats[0]['game_name'],
            'mapname': mapname,
            'num_players': player_count
        }

    async def get_mod_versions(self, mod):
        query = """
            SELECT
                `updates_{mod}_files`.`fileId` AS file_id,
                MAX(`updates_{mod}_files`.`version`) AS version
            FROM `updates_{mod}`
            INNER JOIN `updates_{mod}_files` ON `fileId` = `updates_{mod}`.`id`
            GROUP BY `updates_{mod}_files`.`fileId`
        """.format(mod=mod)
        logger.debug(f"Performing query: {query}")

        # This is a fix for FAF db updates_* tables shindig. Mod updates should
        # be kept in just a single set of tables, but instead each mod has its
        # own set - and some don't have any, like ladder1v1!
        # As a stopgap, we'll swallow all errors that happen to this query and
        # just return an empty dict. I can't be bothered to make some sort of
        # subquery to check if the table exists, or parse exception string for
        # that - we'll live with that until we make a new replay format.
        try:
            featured_mods = await self._db.execute(query)
        except BookkeepingError as e:
            if mod != "ladder1v1":
                logger.warning((f"Failed to query mod versions for {mod}: {e}"
                                f", not saving them in replay"))
            return {}

        return {str(mod['file_id']): mod['version']
                for mod in featured_mods}
