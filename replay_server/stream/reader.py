import asyncio
from io import RawIOBase
from typing import List, Any, Dict

from replay_server.constants import READ_BUFFER_SIZE, WAIT_STEP
from replay_server.logger import logger
from replay_server.replay_parser.replay_parser.parser import parse
from replay_server.stream.base import ReplayWorkerBase
from replay_server.stream.replay_storage import ReplayStorage
from replay_server.stream.worker_storage import WorkerStorage
from replay_server.stream.writer import ReplayWriter
from replay_server.utils.greatest_common_replay import get_greatest_common_stream

__all__ = ('ReplayReader',)


class ReplayReader(ReplayWorkerBase):
    """
    Handler for reading stream.
    """
    def __init__(self, buffers: List[RawIOBase], *args: List[Any], **kwargs: Dict[Any, Any]):
        super(ReplayReader, self).__init__(*args, **kwargs)

        self.buffers: List[RawIOBase] = []  # List of buffers
        self.body_positions: List[int] = []  # All buffers has position, where data begins

        self.initialize_buffers(buffers)
        logger.debug("Prepared to read stream for %s", self.get_uid())

    def initialize_buffers(self, buffers: List[RawIOBase]):
        """
        Initializes which streams can use, finds replay body start positions.
        """
        # initialize reading positions, headers may differ
        logger.debug("Initializing buffers for %s", self.get_uid())
        for buffer in buffers:
            buffer.seek(0)
            try:
                replay = parse(buffer)
                self.buffers.append(buffer)
                self.body_positions.append(replay['body_offset'])
            except ValueError as e:
                continue

    async def process(self):
        """
        Streams the most common stream of connected players.
        Waits, for data, if buffers are "empty".
        """
        # send "header" information
        logger.info("Reading header for %s", self.get_uid())
        data = self.buffers[0].read(self.body_positions[0])
        self._connection.writer.write(data)
        logger.info("Header for %s with length %s", self.get_uid(), len(data))

        # read common stream
        while True:
            data = get_greatest_common_stream(self.buffers, self.body_positions, self.position, READ_BUFFER_SIZE)
            logger.debug("Reading data for %s with length %s, position %s", self.get_uid(), len(data), self.position)
            if not data:
                # if there are still writers online
                if not self.has_writer_online():
                    logger.debug("Ended reading data for %s. Total length %s", self.get_uid(), self.position)
                    await self._connection.writer.drain()
                    break

                print ('waiting...')
                logger.debug("No data get for %s. Waiting", self.get_uid())
                await asyncio.sleep(WAIT_STEP)
                continue
            print("streaming....")
            logger.debug("Streaming data %s", len(data))
            self._connection.writer.write(data)
            self.position += len(data)

        logger.info("End reading data for %s. Total length %s", self.get_uid(), self.position)

    async def cleanup(self):
        logger.info("Closing buffers for %s", self.get_uid())
        for buffer in self.buffers:
            buffer.close()

        online_workers = WorkerStorage.get_online_workers(self.get_uid())
        if len(online_workers) == 0:
            ReplayStorage.remove_replay_data(self.get_uid())

        logger.info("Closed buffers for %s", self.get_uid())

    def has_writer_online(self):
        """
        Checks if uid has active writter
        """
        workers_online = WorkerStorage.get_online_workers(self.get_uid())
        return any([isinstance(instance, ReplayWriter) for instance in workers_online])
