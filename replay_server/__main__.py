import asyncio

from replay_server.constants import PORT
from replay_server.db_conn import db
from replay_server.logger import logger
from replay_server.server import ReplayServer

if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    logger.info("Checking MySQL DB")
    try:
        # check, that mysql is available and create connection pool
        loop.run_until_complete(asyncio.ensure_future(db.create_pool(loop)))
    except Exception as e:
        logger.exception("Something goes wrong with MySQL")

    logger.info("Started server...")
    server = ReplayServer(PORT)
    start = asyncio.ensure_future(server.start(), loop=loop)
    try:
        loop.run_forever()
        logger.info("Server started")
    except (KeyboardInterrupt, Exception) as e:
        if not isinstance(e, KeyboardInterrupt):
            logger.exception('Unexpected exception during program run')
        logger.info('Stopping server...')
        loop.run_until_complete(server.stop())
    finally:
        loop.close()
        logger.info('Server stopped')
