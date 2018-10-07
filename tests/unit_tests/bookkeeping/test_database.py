import pytest
from tests import docker_faf_db_config
import datetime

from replayserver.bookkeeping.database import Database, ReplayDatabaseQueries
from replayserver.errors import BookkeepingError


docker_db_config = {
    "config_db_host": docker_faf_db_config["host"],
    "config_db_port": docker_faf_db_config["port"],
    "config_db_user": docker_faf_db_config["user"],
    "config_db_password": docker_faf_db_config["password"],
    "config_db_name": docker_faf_db_config["db"]
}


# TODO - no tests modifying the db just yet until we set up a db reset fixture
@pytest.mark.asyncio
async def test_database_ok_query():
    db = Database.build(**docker_db_config)

    await db.start()
    result = await db.execute('SELECT * FROM login')
    assert 1 in [r['id'] for r in result]
    await db.close()


@pytest.mark.asyncio
async def test_database_bad_query():
    db = Database.build(**docker_db_config)

    await db.start()
    with pytest.raises(BookkeepingError):
        await db.execute('SELECT * glablagradargh')
    await db.close()


@pytest.mark.asyncio
async def test_database_query_at_bad_time():
    db = Database.build(**docker_db_config)

    with pytest.raises(BookkeepingError):
        await db.execute('SELECT * FROM login')
    await db.start()
    await db.close()
    with pytest.raises(BookkeepingError):
        await db.execute('SELECT * FROM login')


@pytest.mark.asyncio
async def test_queries_get_teams(mock_database):
    queries = ReplayDatabaseQueries(mock_database)
    await mock_database.add_mock_game((1, 1, 1),
                                      [(1, 1), (2, 2)])
    teams = await queries.get_teams_in_game(1)
    assert teams == {1: ["user1"], 2: ["user2"]}


@pytest.mark.asyncio
async def test_queries_get_game_stats(mock_database):
    queries = ReplayDatabaseQueries(mock_database)
    await mock_database.add_mock_game((1, 1, 1),
                                      [(1, 1), (2, 2)])
    teams = await queries.get_game_stats(1)
    assert teams == {
        'featured_mod': 'faf',
        'game_type': '0',
        'recorder': 'user1',
        'host': 'user1',
        'launched_at': datetime.datetime(2000, 1, 1, 0, 0),
        'game_end': datetime.datetime(2000, 1, 1, 0, 0),
        'title': 'Name of the game',
        'mapname': 'scmp_1',
        'map_file_path': 'maps/scmp_1.zip',
        'num_players': 2
    }
