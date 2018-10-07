import pytest
from tests import docker_faf_db_config

from replayserver.bookkeeping.database import Database
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
