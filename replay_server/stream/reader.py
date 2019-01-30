import asyncio
from io import RawIOBase
from typing import List, Any, Dict

from replay_parser.body import ReplayBody
from replay_parser.constants import CommandStates
from replay_parser.reader import ReplayReader as Reader
from replay_parser.replay import parse

from replay_server.constants import WAIT_STEP, TICK_COUNT_TIMEOUT
from replay_server.logger import logger
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
        self.tick = 0

        self.replay_body_parser: ReplayBody = ReplayBody(
            Reader(),
            parse_commands={
                CommandStates.Advance,
                CommandStates.SetCommandSource,
                CommandStates.CommandSourceTerminated,
            }
        )

        self.initialize_buffers(buffers)
        logger.debug("<%s> Prepared to read stream for %s", self._connection, self.get_uid())

    def initialize_buffers(self, buffers: List[RawIOBase]) -> None:
        """
        Initializes which streams can use, finds replay body start positions.
        """
        # initialize reading positions, headers may differ
        logger.debug("<%s> Initializing buffers for %s", self._connection, self.get_uid())
        for buffer in buffers:
            buffer.seek(0)
            try:
                replay = parse(buffer, parse_body=False)
                self.buffers.append(buffer)
                self.body_positions.append(replay['body_offset'])
            except ValueError as e:
                continue

    async def process_body(self) -> None:
        """
        Continuously parses replay body, checks for current ticks, pauses, resumes of the game.
        """
        try:
            while True:
                await asyncio.sleep(WAIT_STEP)
                has_writer_online = self.has_writer_online()
                data = get_greatest_common_stream(self.buffers, self.body_positions, self.position)

                logger.debug("<%s> Reading data for %s with length %s, position %s",
                             self._connection, self.get_uid(), len(data), self.position)
                if not data:
                    if not has_writer_online:
                        break
                    logger.debug("<%s> No data get for %s. Waiting", self._connection, self.get_uid())
                    await asyncio.sleep(WAIT_STEP)
                    continue

                size_to_read = self.get_size_to_read(data, has_writer_online)
                logger.debug("<%s> Streaming data %s", self._connection, size_to_read)
                if size_to_read > 0:
                    self._connection.writer.write(data[:size_to_read])
                    self.position += size_to_read
                await self._connection.writer.drain()

        finally:
            logger.debug("<%s> Ended reading data for %s. Total length %s",
                         self._connection, self.get_uid(), self.position)
            await self._connection.writer.drain()

    def get_size_to_read(self, data: bytes, has_writer_online: bool) -> int:
        """
        Compute amount of ticks that we can read.
        1. parse to max tick
        2. read to max tick - TICK_COUNT_TIMEOUT
        Return size to read.
        """
        max_tick = self.tick
        # find greatest tick
        for tick, _, _ in self.replay_body_parser.continuous_parse(data):
            max_tick = tick
        max_tick = max(max_tick - TICK_COUNT_TIMEOUT, 0)

        self.replay_body_parser.tick = self.tick
        self.replay_body_parser.replay_reader.seek(0)

        size_to_read = 0
        for tick, _, replay_data in self.replay_body_parser.continuous_parse():
            read_length = len(replay_data)

            # parser couldn't understand, even if we have data
            if not read_length:
                break

            if tick > max_tick and has_writer_online:
                break

            self.tick = tick
            size_to_read += read_length

        return size_to_read

    async def process(self) -> None:
        """
        Streams the most common stream of connected players.
        Waits, for data, if buffers are "empty".
        """
        # send "header" information
        logger.info("<%s> Reading header for %s", self._connection, self.get_uid())
        data = self.buffers[0].read(self.body_positions[0])
        self._connection.writer.write(data)
        logger.info("<%s> Header for %s with length %s", self._connection, self.get_uid(), len(data))

        # read common stream
        await self.process_body()

        logger.info("<%s> End reading data for %s. Total length %s",
                    self._connection, self.get_uid(), self.position)

    async def cleanup(self) -> None:
        logger.info("<%s> Closing buffers for %s", self._connection, self.get_uid())
        for buffer in self.buffers:
            buffer.close()

        # remove current worker from storage
        WorkerStorage.remove_worker(self.get_uid(), self)

        online_workers = WorkerStorage.get_online_workers(self.get_uid())
        if len(online_workers) == 0:
            ReplayStorage.remove_replay_data(self.get_uid())

        logger.info("<%s> Closed buffers for %s", self._connection, self.get_uid())

    def has_writer_online(self) -> bool:
        """
        Checks if uid has active writter
        """
        workers_online = WorkerStorage.get_online_workers(self.get_uid())
        return any([isinstance(instance, ReplayWriter) for instance in workers_online])
