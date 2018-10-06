import asyncio
import aiomysql


async def clear_db():
    conn = await aiomysql.connect(host='172.19.0.2', port=3306,
                                  user='root', password='banana', db='faf')

    cur = await conn.cursor()
    await cur.execute(
        "SELECT CONCAT('DELETE FROM ', table_schema, '.', table_name, ';')"
        " FROM information_schema.tables"
        " WHERE table_type = 'BASE TABLE'"
        " AND table_schema = 'faf';")
    r = await cur.fetchall()
    q = "\n".join([i[0] for i in r if not "schema_version" in i[0]])
    await cur.execute("SET FOREIGN_KEY_CHECKS=0;")
    await cur.execute(q)
    await cur.execute("SET FOREIGN_KEY_CHECKS=1;")
    await cur.close()
    conn.close()


f = asyncio.ensure_future(clear_db())
asyncio.get_event_loop().run_until_complete(f)
