import asyncio

from replay_server.connection import ReplayConnection
from replay_server.db_conn import db
from replay_server.logger import logger
from replay_server.stream.factory import WorkerFactory
from replay_server.stream.worker_storage import WorkerStorage

__all__ = ('ReplayServer',)


class ReplayServer:
    """
    Handling requests to process
    """
    def __init__(self, port):
        self._port = port
        self._server = None

    async def handle_connection(self, reader, writer):
        """
        Main method, that handle db_connection
        """
        connection = ReplayConnection(reader, writer)
        logger.info("<%s> Connection established...", connection)
        replay_worker = None
        try:
            request_type = await connection.determine_type()
            uid, replay_name = await connection.get_replay_name()
            info = "Saving" if request_type == 0 else "Serving"
            logger.info("<%s> %s for %s, '%s'", connection, info, str(uid), replay_name)

            replay_worker = WorkerFactory.get_worker(uid, request_type, connection)
            WorkerStorage.add_worker(uid, replay_worker)
            await replay_worker.run()
        except ConnectionError as e:
            logger.exception("<%s> Connection problems occurs!", connection)
            if replay_worker:
                await replay_worker.cleanup()
            connection.writer.write(str(e).encode('raw_unicode_escape'))
        except Exception as e:
            logger.exception("<%s> Something goes terribly wrong!", connection)
            if replay_worker:
                await replay_worker.cleanup()
            connection.writer.write("Wrong request: ".encode('raw_unicode_escape'))
            connection.writer.write(str(e).encode('raw_unicode_escape'))
            await connection.close()
        finally:
            try:
                await connection.close()
            except Exception:
                logger.exception("<%s> Something goes terribly wrong during connection close", connection)

    async def start(self):
        """
        Start server on port
        """
        logger.info("Starting server on port %s", self._port)
        self._server = await asyncio.streams.start_server(self.handle_connection, port=self._port)

    async def stop(self):
        """
        Stop server and try to cleanup
        """
        logger.info("Stopping server on port %s", self._port)

        await db.close()
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        logger.info("Successfully closed")

