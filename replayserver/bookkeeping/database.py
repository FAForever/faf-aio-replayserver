import aiomysql
from aiomysql import create_pool


class Database:
    def __init__(self, connection_pool):
        self._connection_pool = connection_pool

    @classmethod
    async def build(cls, config_db_host, config_db_port, config_db_user,
                    config_db_password, config_db_name, **kwargs):
        pool = await create_pool(host=config_db_host,
                                 port=config_db_port,
                                 user=config_db_user,
                                 password=config_db_password,
                                 db=config_db_name)
        return cls(pool)

    async def execute(self, query, params=[]):
        async with self._connection_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query, *params)
                return await cur.fetchall()

    async def close(self):
        self._connection_pool.close()
        await self._connection_pool.wait_closed()


class ReplayDatabaseQueries:
    def __init__(self, db):
        self._db = db

    async def get_players_in_game(self, game_id):
        query = """
            SELECT
                `login`.`login` AS login,
                `game_player_stats`.`team` AS team
            FROM `game_stats`
            INNER JOIN `game_player_stats`
              ON `game_player_stats`.`gameId` = `game_stats`.`id`
            INNER JOIN `login`
              ON `login`.id = `game_player_stats`.`playerId`
            WHERE `game_stats`.`id` = %s
        """
        return await self._db.execute(query, (game_id,))

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
                `map`.`display_name` as map_name,
                `map_version`.`filename` AS file_name,
                `game_player_stats`.`playerId` AS player_id,
                `game_player_stats`.`AI` AS ai
            FROM `game_stats`
            INNER JOIN `game_player_stats`
              ON `game_player_stats`.`gameId` = `game_stats`.`id`
            INNER JOIN `map`
              ON `game_stats`.`mapId` = `map`.`id`
            INNER JOIN `map_version`
              ON `map_version`.`map_id` = `map`.`id`
            INNER JOIN `login`
              ON `login`.id = `game_stats`.`host`
            INNER JOIN  `game_featuredMods`
              ON `game_stats`.`gameMod` = `game_featuredMods`.`id`
            WHERE `game_stats`.`id` = %s
        """
        return await self._db.execute(query, (game_id,))

    async def get_featured_mod_version(self, mod):
        query = """
            SELECT
                `updates_{mod}_files`.`fileId` AS file_id,
                MAX(`updates_{mod}_files`.`version`) AS version
            FROM `updates_{mod}`
            LEFT JOIN `updates_{mod}_files` ON `fileId` = `updates_{mod}`.`id`
            GROUP BY `updates_{mod}_files`.`fileId`
        """.format(mod=mod)
        return await self._db.execute(query)
