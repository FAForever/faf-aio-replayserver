import pytest

from replay_server.db_conn import db


@pytest.yield_fixture
async def db_connection_fixture(event_loop):
    """
    Fixture that creates transaction and then at the end rollbacks everything.
    """
    await db.create_pool(loop=event_loop)
    conn = await db.get_pool().acquire()
    await conn.begin()
    yield conn
    await conn.rollback()
    conn.close()
    await conn.ensure_closed()
    await db.close()


@pytest.yield_fixture
async def cursor(db_connection_fixture):
    db.set_testing_conn(db_connection_fixture)
    yield db


@pytest.fixture
async def db_login(cursor):
    await cursor.execute("INSERT INTO login (login, email) VALUES ('user1', 'user1@email.cc')")


@pytest.fixture
async def db_game_featured_mods(cursor):
    await cursor.execute("""
        INSERT INTO `game_featuredMods` (id, gamemod, description, name, publish, order)
        VALUES
        (0, "faf", "description", "FAF", 1, 0),
        (1, "murderparty", "description", "Murder Party", 1, 10),
        (4, "nomads", "description", "Nomads", 1, 10),
        (5, "labwars", "description", "LABwars", 1, 10),
        (6, "ladder1v1", "description", "Ladder1v1", 0, 10),
        (12, "xtremewars", "description", "Xtreme Wars", 1, 10),
        (14, "diamond", "description", "Diamond", 1, 10),
        (16, "phantomx", "description", "Phantom-X", 1, 10),
        (18, "vanilla", "description", "Vanilla", 1, 10),
        (20, "koth", "description", "King of the Hill", 1, 10),
        (21, "claustrophobia", "description", "Claustrophobia", 1, 10),
        (24, "gw", "description", "Galactic War", 0, 10),
        (25, "coop", "description", "Coop", 0, 10),
        (26, "matchmaker", "description", "Matchmaker", 0, 10),
        (27, "fafbeta", "description", "FAF Beta", 1, 2),
        (28, "fafdevelop", "description", "FAF Develop", 1, 11),
        (29, "equilibrium", "description", "Equilibrium", 1, 3)
        ON DUPLICATE KEY UPDATE `id` = `id`
    """)


@pytest.fixture
async def db_table_map(cursor):
    await cursor.execute("""
        INSERT INTO `map` (`id`, `display_name`, `map_type`, `battle_type`, `author`,
                           `average_review_score`, `reviews`, `create_time`, `update_time`)
        VALUES (1, 'scmp_016', '?', '?', NULL, '0', '0', now(), now())
        ON DUPLICATE KEY UPDATE `id` = `id`
    """)
    await cursor.execute("""
        INSERT INTO `map_version` (`description`, `max_players`, `width`, `height`, `version`,
                                   `filename`, `ranked`, `hidden`, `map_id`, `create_time`, `update_time`)
        VALUES (NULL, '2', '20', '20', '1', 'maps/scmp_016.zip', '1', '0', '1', now(), now())
        ON DUPLICATE KEY UPDATE `id` = `id`
    """)


@pytest.fixture
async def db_user(cursor):
    await cursor.execute("""
        INSERT INTO `login` (`id`, `login`, `password`, `email`)
        VALUES (1, "user1", "banana", "user1@banana.cc")
        ON DUPLICATE KEY UPDATE `id` = `id`
    """)


@pytest.fixture
async def db_game_stats(cursor, replay_id, db_user):
    await cursor.execute("""
        INSERT INTO `game_stats` (`id`, `starttime`, `endtime`, `gametype`, `gamemod`, 
                                  `host`, `mapid`, `gamename`, `validity`)
        VALUES ({}, now(), now(), '0', 0, 1, 1, "Name of the game", 1)
        ON DUPLICATE KEY UPDATE `id` = `id`
    """.format(replay_id))


@pytest.fixture
async def db_game_player_stats(cursor, replay_id, db_game_stats):
    await cursor.execute("""
        INSERT INTO `game_player_stats` (`id`, `gameid`, `playerid`, `ai`, `faction`, 
                                         `color`, `team`, `place`, `mean`, `deviation`)
        VALUES (1, {}, 1, 0, 1, 1, 1, 1, 0, 0)
        ON DUPLICATE KEY UPDATE `id` = `id`
    """.format(replay_id))


@pytest.fixture
async def db_updates_faf(cursor):
    await cursor.execute("""
        INSERT INTO updates_faf(`id`, `filename`, `path`)
        VALUES (1, "scmp_016", "maps/scmp_016.zip")
        ON DUPLICATE KEY UPDATE `id` = `id`
    """)
    await cursor.execute("""
        INSERT INTO updates_faf_files(`id`, `fileId`, `version`, `name`, `md5`, `obselete`)
        VALUES (1, 1, 1, "banana", "72B302BF297A228A75730123EFEF7C41", 1)
        ON DUPLICATE KEY UPDATE `id` = `id`
    """)


@pytest.fixture
async def db_replay(db_user, db_table_map, db_game_stats, db_game_player_stats, db_updates_faf):
    pass
