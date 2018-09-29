import aiomysql
from aiomysql import create_pool
from replayserver.errors import BookkeepingError


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
            WHERE `game_stats`.`id` = %s
        """
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
        game_stats = await self._db.execute(query, (game_id,))
        if not game_stats:
            raise BookkeepingError("No game stats found")
        return {
            'featured_mod': game_stats[0]['game_mod'],
            'game_type': game_stats[0]['game_type'],
            'recorder': game_stats[0]['host'],
            'host': game_stats[0]['host'],
            'launched_at': game_stats[0]['start_time'],
            'game_end': game_stats[0]['end_time'],
            'title': game_stats[0]['game_name'],
            'mapname': game_stats[0]['map_name'],
            'map_file_path': game_stats[0]['file_name'],
            'num_players': len(game_stats)
        }

    async def get_mod_versions(self, mod):
        query = """
            SELECT
                `updates_{mod}_files`.`fileId` AS file_id,
                MAX(`updates_{mod}_files`.`version`) AS version
            FROM `updates_{mod}`
            LEFT JOIN `updates_{mod}_files` ON `fileId` = `updates_{mod}`.`id`
            GROUP BY `updates_{mod}_files`.`fileId`
        """.format(mod=mod)
        featured_mods = await self._db.execute(query)
        return {str(mod['file_id']): mod['version']
                for mod in featured_mods}

    async def register_replay(self, game_id):
        query = "INSERT INTO `game_replays` (UID) VALUES (%s)"
        await self._db.execute(query)
