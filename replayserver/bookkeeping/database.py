import aiomysql
from aiomysql import create_pool


class Database:
    def __init__(self, connection_pool):
        self._connection_pool = connection_pool

    @classmethod
    async def build(cls, config_db_host, config_db_port, config_db_user,
                    config_db_password, config_db_name):
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
