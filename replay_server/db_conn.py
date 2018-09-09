import asyncio
from typing import List

import aiomysql
from aiomysql import create_pool, Pool

from replay_server.constants import MYSQL_DNS


__all__ = ('db',)


class DB:
    """
    Handles db connections
    """
    def __init__(self):
        self.connection_pool: Pool = None
        self.testing_conn = None

    async def create_pool(self, loop: asyncio.AbstractEventLoop) -> None:
        self.connection_pool = await create_pool(loop=loop, **MYSQL_DNS)

    def get_pool(self):
        return self.connection_pool

    def set_testing_conn(self, testing_conn):
        self.testing_conn = testing_conn

    async def execute(self, query: str, params: List = None):
        if not params:
            params = []

        if self.testing_conn:
            async with self.testing_conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query, *params)
                return await cur.fetchall()

        async with self.connection_pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query, *params)
                return await cur.fetchall()

    async def close(self):
        if self.connection_pool:
            self.connection_pool.close()
            self.connection_pool = None


db = DB()
