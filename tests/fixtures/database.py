import pytest
import asynctest
import os
import aiomysql
from aiomysql import DatabaseError

from replayserver.errors import BookkeepingError


class MockDatabase:
    async def mock_start(self):
        host = os.environ.get("FAF_STACK_DB_IP", "172.19.0.2")
        self._conn = await aiomysql.connect(
            host=host, port=3306, user='root', password='banana', db='faf')
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
