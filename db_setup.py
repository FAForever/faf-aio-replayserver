import asyncio
import aiomysql
from tests import docker_faf_db_config


async def clear_db(cursor):
    await cursor.execute(
        "SELECT CONCAT('DELETE FROM ', table_schema, '.', table_name, ';')"
        " FROM information_schema.tables"
        " WHERE table_type = 'BASE TABLE'"
        " AND table_schema = 'faf';")
    r = await cursor.fetchall()
    q = "\n".join([i[0] for i in r if "schema_version" not in i[0]])
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
    data = [(i, "user{}".format(i), "foo", "bar{}".format(i))
            for i in nums]
    await cursor.executemany("""
        INSERT INTO login (id, login, password, email) VALUES
        (%s, %s, %s, %s)
        """, data)


async def db_mock_map(cursor, nums):
    data = [(i, 'scmp_{}'.format(i), '?', '?', None,
            0, 0, '2000-01-01 00:00:00', '2000-01-01 00:00:00')
            for i in nums]
    await cursor.executemany("""
        INSERT INTO `map`
            (`id`, `display_name`, `map_type`, `battle_type`, `author`,
             `average_review_score`, `reviews`, `create_time`, `update_time`)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, data)
    data = [(None, 8, 100, 100, 1, 'maps/scmp_{}.zip'.format(i), 1, 0, i,
             '2000-01-01 00:00:00', '2000-01-01 00:00:00')
            for i in nums]
    await cursor.executemany("""
        INSERT INTO `map_version`
            (`description`, `max_players`, `width`, `height`, `version`,
             `filename`, `ranked`, `hidden`, `map_id`,
             `create_time`, `update_time`)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, data)


async def db_mock_updates_faf(cursor, nums):
    data = [(i, "scmp_{}".format(i), "maps/scmp_{}.zip".format(i))
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


async def db_mock_game_stats(cursor, replay_id, map_id, host_id):
    await cursor.execute("""
        INSERT INTO `game_stats`
            (`id`, `starttime`, `endtime`, `gametype`,
             `gamemod`, `host`, `mapid`, `gamename`, `validity`)
        VALUES
            ({replay_id}, '2000-01-01 00:00:00', '2000-01-01 00:00:00', '0',
             1, {host_id}, {map_id}, "Name of the game", 1)
    """.format(replay_id=replay_id, map_id=map_id, host_id=host_id))


async def db_mock_game_player_stats(cursor, replay_id, player_id, team):
    await cursor.execute("""
        INSERT INTO `game_player_stats`
            (`id`, `gameid`, `playerid`, `ai`, `faction`,
             `color`, `team`, `place`, `mean`, `deviation`)
        VALUES
            (NULL, {replay_id}, {player_id}, 0, 1,
             1, {team}, 1, 0, 0)
    """.format(replay_id=replay_id, player_id=player_id, team=team))


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


async def add_game(cursor, game, players):
    replay_id = game[0]
    await db_mock_game_stats(cursor, *game)
    for player in players:
        await db_mock_game_player_stats(cursor, replay_id, *player)


async def populate():
    conn = await aiomysql.connect(
        host=docker_faf_db_config['host'],
        port=docker_faf_db_config['port'],
        user=docker_faf_db_config['user'],
        password=docker_faf_db_config['password'],
        db=docker_faf_db_config['db'])
    cur = await conn.cursor()
    await prepare_default_data(cur)
    await add_game(cur, (1, 1, 1),
                   [(1, 1), (2, 2), (3, 3)])
    await add_game(cur, (2, 2, 1),
                   [(1, 1), (2, 1), (3, 2), (4, 2)])
    await cur.close()
    await conn.commit()
    conn.close()


f = asyncio.ensure_future(populate())
asyncio.get_event_loop().run_until_complete(f)
if f.exception() is not None:
    raise f.exception()
