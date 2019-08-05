from asyncio.locks import Lock

import aiomysql
import asynctest
import pytest
from aiomysql import DatabaseError
from replayserver.errors import BookkeepingError
from tests import docker_faf_db_config, test_db


class MockDatabase:
    async def mock_start(self):
        self._conn = await aiomysql.connect(
            host=docker_faf_db_config['host'],
            port=docker_faf_db_config['port'],
            user=docker_faf_db_config['user'],
            password=docker_faf_db_config['password'],
            db=docker_faf_db_config['db'])
        await self._conn.begin()
        # aiomysql has threadsafety 1, but we still want to use a single
        # connection for rolling back everything
        self._lock = Lock()

    async def mock_close(self):
        async with self._lock:
            await self._conn.rollback()
            self._conn.close()

    async def start(self):
        pass

    async def execute(self, query, params=[]):
        try:
            async with self._lock:
                async with self._conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute(query, *params)
                    return await cur.fetchall()
        except (DatabaseError, RuntimeError) as e:
            raise BookkeepingError from e

    async def stop(self):
        pass

    async def _db_mock_game_stats(self, cursor, replay_id, map_id, host_id):
        # The 'mapid' field refers to the 'map_version' table, not 'map'!
        mid = test_db.MAP_VERSION_ID_OFFSET + map_id
        await cursor.execute(f"""
            INSERT INTO `game_stats`
                (`id`, `starttime`, `endtime`, `gametype`,
                 `gamemod`, `host`, `mapid`, `gamename`, `validity`)
            VALUES
                ({replay_id}, '2001-01-01 00:00:00', '2001-01-02 00:00:00',
                 '0', 1, {host_id}, {mid}, "Name of the game", 1)
        """)

    async def _db_mock_game_player_stats(self, cursor,
                                         replay_id, player_id, team, ai=False):
        await cursor.execute(f"""
            INSERT INTO `game_player_stats`
                (`id`, `gameid`, `playerid`, `ai`, `faction`,
                 `color`, `team`, `place`, `mean`, `deviation`)
            VALUES
                (NULL, {replay_id}, {player_id}, {ai}, 1,
                 1, {team}, 1, 0, 0)
        """)

    async def _db_get_game_ticks(self, cursor, replay_id):
        await cursor.execute(f"""
            SELECT `replay_ticks` FROM `game_stats`
                WHERE `game_stats`.`id` = {replay_id}
        """)
        return await cursor.fetchone()

    async def add_mock_game(self, game, players):
        cur = await self._conn.cursor()
        replay_id = game[0]
        await self._db_mock_game_stats(cur, *game)
        for player in players:
            await self._db_mock_game_player_stats(cur, replay_id, *player)

    async def get_game_ticks(self, game_id):
        async with self._conn.cursor() as cur:
            result = await self._db_get_game_ticks(cur, game_id)
            return result[0]


@pytest.fixture
async def mock_database():
    mock_db = MockDatabase()
    wrap = asynctest.Mock(wraps=mock_db, spec=mock_db)
    await wrap.mock_start()
    try:
        yield wrap
    finally:
        await wrap.mock_close()
