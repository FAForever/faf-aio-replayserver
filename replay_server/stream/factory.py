from io import FileIO

from replay_server.connection import ReplayConnection
from replay_server.constants import RequestType
from replay_server.logger import logger
from replay_server.stream.reader import ReplayReader
from replay_server.stream.replay_storage import ReplayStorage
from replay_server.stream.writer import ReplayWriter
from replay_server.utils.paths import get_temp_replay_path

__all__ = ('WorkerFactory',)


class WorkerFactory:
    """
    Create workers
    """
    @classmethod
    def get_worker(cls, uid: int, type_: int, connection: ReplayConnection):
        """
        Factory, that creates replay with worker.
        For reading returns biggest replay from the server.
        For writing operations, it creates writer, that saves stream into temporary file.

        :param int uid: stream ID
        :param int type_: type of action (writing or reading replay)
        :param ReplayConnection connection: replay db_connection instance
        """
        replay_worker_class = None
        kwargs = {
            "connection": connection
        }

        if type_ == RequestType.READER:
            replays = ReplayStorage.get_replays(uid)
            if not replays:
                raise ConnectionError("Can't read a non existing stream!")
            kwargs['buffers'] = [FileIO(filename_, "rb") for filename_ in replays.keys()]
            replay_worker_class = ReplayReader

        elif type_ == RequestType.WRITER:
            file_path = get_temp_replay_path(uid)
            file_ = FileIO(file_path, "w")
            kwargs['buffer'] = file_
            replay_worker_class = ReplayWriter
            ReplayStorage.set_replay(uid, file_path, file_)

        return replay_worker_class(**kwargs)
