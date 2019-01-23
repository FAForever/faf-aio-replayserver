from io import RawIOBase
from typing import List, Any, Dict

from replay_server.constants import WRITE_BUFFER_SIZE
from replay_server.logger import logger
from replay_server.saver import save_replay
from replay_server.stream.base import ReplayWorkerBase
from replay_server.stream.replay_storage import ReplayStorage
from replay_server.stream.worker_storage import WorkerStorage

__all__ = ('ReplayWriter',)


class ReplayWriter(ReplayWorkerBase):
    """
    Read from stream db_connection and stores data into the buffer.
    """

    def __init__(self, buffer: RawIOBase, *args: List[Any], **kwargs: Dict[Any, Any]):
        super(ReplayWriter, self).__init__(*args, **kwargs)
        self.buffer: RawIOBase = buffer
        self.position: int = 0
        logger.info("<%s> Prepared to save stream %s for", self._connection, self.get_uid())

    async def process(self) -> None:
        """
        Saves stream into the file
        """
        logger.info("<%s> Reading save stream for %s", self._connection, self.get_uid())
        while True:
            data = await self._connection.reader.read(WRITE_BUFFER_SIZE)
            logger.debug("<%s> Write len data %s on position %s", self._connection, len(data), self.position)
            if not data:
                break

            self.feed(self.position, data)
            self.position += len(data)

        logger.info("<%s> Finished save stream for %s with length %s", self._connection, self.get_uid(), self.position)

    def feed(self, offset: int, data: bytes) -> None:
        """
        Writes data into the buffer
        """
        data_end = offset + len(data)

        start = self.position - data_end
        if start < data_end:
            self.buffer.write(data[start:])
            self.position = data_end

    async def cleanup(self) -> None:
        """
        Closes buffers, removes worker from active workers, saves replay, if there is no writers.
        """
        logger.info("<%s> Closing buffer for for %s", self._connection, self.get_uid())
        self.buffer.close()

        # remove current worker from storage
        WorkerStorage.remove_worker(self.get_uid(), self)

        # We will save, if there is no writers
        online_workers = WorkerStorage.get_online_workers(self.get_uid())

        writers_online = any([isinstance(online_processor, ReplayWriter) for online_processor in online_workers])
        if not writers_online and ReplayStorage.has_replays(self.get_uid()):
            logger.info("<%s> There is no writers online, saving replay", self._connection)
            await save_replay(
                self.get_uid(),
                list(ReplayStorage.get_replays(self.get_uid()).keys()),
                ReplayStorage.get_replay_start_time(self.get_uid()),
            )

        if len(online_workers) == 0:
            ReplayStorage.remove_replay_data(self.get_uid())

        logger.info("<%s> Closed buffer for for %s", self._connection, self.get_uid())
