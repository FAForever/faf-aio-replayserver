import pytest
import asynctest
import aiomysql
from aiomysql import DatabaseError

from replayserver.errors import BookkeepingError
from tests import docker_faf_db_config


class MockDatabase:
    async def mock_start(self):
        self._conn = await aiomysql.connect(
            host=docker_faf_db_config['host'],
            port=docker_faf_db_config['port'],
            user=docker_faf_db_config['user'],
            password=docker_faf_db_config['password'],
            db=docker_faf_db_config['db'])
        self._conn.begin()

    async def mock_close(self):
        self._conn.rollback()
        self._conn.close()

    async def start(self):
        pass

    async def execute(self, query, params=[]):
        try:
            cur = await self._conn.cursor()
            await cur.execute(query, *params)
            return await cur.fetchall()
        except (DatabaseError, RuntimeError) as e:
            raise BookkeepingError from e

    async def close(self):
        pass


@pytest.fixture
async def mock_database():
    mock_db = MockDatabase()
    wrap = asynctest.Mock(wraps=mock_db, spec=mock_db)
    await wrap.mock_start()
    try:
        yield wrap
    finally:
        await wrap.mock_close()
