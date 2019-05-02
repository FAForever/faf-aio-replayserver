import asyncio
import aiomysql
from docker_db_config import docker_faf_db_config
from test_db import MAP_VERSION_ID_OFFSET, MAP_ID_OFFSET, \
    SPECIAL_GAME_NO_END_TIME_ID


async def clear_db(cursor):
    await cursor.execute(
        "SELECT CONCAT('DELETE FROM ', table_schema, '.', table_name, ';')"
        " FROM information_schema.tables"
        " WHERE table_type = 'BASE TABLE'"
        " AND table_schema = 'faf';")
    r = await cursor.fetchall()
    q = "\n".join([i[0] for i in r if "schema_version" not in i[0]])
    if not q:
        return
    await cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
    await cursor.execute(q)
    await cursor.execute("SET FOREIGN_KEY_CHECKS=1;")


async def db_mock_featured_mods(cursor):
    await cursor.execute("""
        INSERT INTO `game_featuredMods`
            (id, gamemod, description, name, publish)
        VALUES
            (1, "faf", "description", "FAF", 1),
            (2, "murderparty", "description", "Murder Party", 1),
            (4, "nomads", "description", "Nomads", 1),
            (5, "labwars", "description", "LABwars", 1),
            (6, "ladder1v1", "description", "Ladder1v1", 0),
            (12, "xtremewars", "description", "Xtreme Wars", 1),
            (14, "diamond", "description", "Diamond", 1),
            (16, "phantomx", "description", "Phantom-X", 1),
            (18, "vanilla", "description", "Vanilla", 1),
            (20, "koth", "description", "King of the Hill", 1),
            (21, "claustrophobia", "description", "Claustrophobia", 1),
            (24, "gw", "description", "Galactic War", 0),
            (25, "coop", "description", "Coop", 0),
            (26, "matchmaker", "description", "Matchmaker", 0),
            (27, "fafbeta", "description", "FAF Beta", 1),
            (28, "fafdevelop", "description", "FAF Develop", 1),
            (29, "equilibrium", "description", "Equilibrium", 1)
    """)


async def db_mock_game_validity(cursor):
    await cursor.execute("""
        INSERT INTO game_validity
            (id, message)
        VALUES
            (1, "All fine!")
    """)


async def db_mock_login(cursor, nums):
    data = [(i, f"user{i}", "foo", f"bar{i}")
            for i in nums]
    await cursor.executemany("""
        INSERT INTO login (id, login, password, email) VALUES
        (%s, %s, %s, %s)
        """, data)


async def db_mock_map(cursor, nums):
    # We offset map and map_version to catch errors if we accidentally join
    # with the wrong one
    map_ids = [MAP_ID_OFFSET + i for i in nums]
    map_version_ids = [MAP_VERSION_ID_OFFSET + i for i in nums]

    data = [(mi, f'scmp_{i}', '?', '?', None,
             0, 0, '2000-01-01 00:00:00', '2000-01-02 00:00:00')
            for i, mi in zip(nums, map_ids)]
    await cursor.executemany("""
        INSERT INTO `map`
            (`id`, `display_name`, `map_type`, `battle_type`, `author`,
             `average_review_score`, `reviews`, `create_time`, `update_time`)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, data)

    data = [(vi, None, 8, 100, 100, 1,
             f'maps/scmp_{i}.zip', 1, 0, mi,
             '2000-01-03 00:00:00', '2000-01-04 00:00:00')
            for i, vi, mi in zip(nums, map_version_ids, map_ids)]
    await cursor.executemany("""
        INSERT INTO `map_version`
            (`id`, `description`, `max_players`, `width`, `height`, `version`,
             `filename`, `ranked`, `hidden`, `map_id`,
             `create_time`, `update_time`)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, data)


async def db_mock_updates_faf(cursor, nums):
    data = [(i, f"scmp_{i}", f"maps/scmp_{i}.zip")
            for i in nums]
    await cursor.executemany("""
        INSERT INTO updates_faf
            (`id`, `filename`, `path`)
        VALUES
            (%s, %s, %s)
    """, data)
    data = [(i, i, 1, "banana", "72B302BF297A228A75730123EFEF7C41", 1)
            for i in nums]
    await cursor.executemany("""
        INSERT INTO updates_faf_files
            (`id`, `fileId`, `version`, `name`, `md5`, `obselete`)
        VALUES
            (%s, %s, %s, %s, %s, %s)
    """, data)


async def db_mock_special_games(cursor):
    # Game without end time
    await cursor.execute(f"""
        INSERT INTO `game_stats`
            (`id`, `starttime`, `endtime`, `gametype`,
             `gamemod`, `host`, `mapid`, `gamename`, `validity`)
        VALUES
            ({SPECIAL_GAME_NO_END_TIME_ID}, '2001-01-01 00:00:00', NULL,
             '0', 1, 1, 1, "Name of the game", 1)
    """)
    # An otherwise valid game must have players
    await cursor.execute(f"""
        INSERT INTO `game_player_stats`
            (`id`, `gameid`, `playerid`, `ai`, `faction`,
             `color`, `team`, `place`, `mean`, `deviation`)
        VALUES
            (NULL, {SPECIAL_GAME_NO_END_TIME_ID}, 1, False, 1,
             1, 1, 1, 0, 0)
    """)


async def prepare_default_data(cursor):
    """
    Stuff like required related tables, a handful of mock users and maps.
    Every test needs it, and it should be sufficient for every test.
    """
    await clear_db(cursor)
    await db_mock_featured_mods(cursor)
    await db_mock_game_validity(cursor)
    await db_mock_login(cursor, range(1, 11))
    await db_mock_map(cursor, range(1, 11))
    await db_mock_updates_faf(cursor, range(1, 11))
    await db_mock_special_games(cursor)


async def populate():
    conn = await aiomysql.connect(
        host=docker_faf_db_config['host'],
        port=docker_faf_db_config['port'],
        user=docker_faf_db_config['user'],
        password=docker_faf_db_config['password'],
        db=docker_faf_db_config['db'])
    cur = await conn.cursor()
    await prepare_default_data(cur)
    await cur.close()
    await conn.commit()
    conn.close()


f = asyncio.ensure_future(populate())
asyncio.get_event_loop().run_until_complete(f)
if f.exception() is not None:
    raise f.exception()
