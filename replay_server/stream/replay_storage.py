import os
from io import FileIO
from time import time
from typing import Dict

from replay_server.logger import logger

__all__ = ('ReplayStorage',)


class ReplayStorage:
    """
    Handle replay data
    """
    # List of byte arrays, where data are stored.
    replay_data: Dict[int, Dict[str, FileIO]] = {}
    replay_start_time: Dict[int, int] = {}

    @classmethod
    def get_replays(cls, uid: int):
        return cls.replay_data[uid]

    @classmethod
    def has_replays(cls, uid: int):
        return uid in cls.replay_data

    @classmethod
    def set_replay(cls, uid: int, file_path: str, file_: FileIO):
        logger.debug("ReplayStorage: Setting path %s", file_path)
        cls.replay_data.setdefault(uid, {})[file_path] = file_
        if uid not in cls.replay_start_time:
            cls.replay_start_time[uid] = int(time())

    @classmethod
    def remove_replay_data(cls, uid: int):
        if uid in cls.replay_data:
            # We have saved replay, so we don't need old replays
            for temporary_file in cls.replay_data[uid]:
                logger.debug("ReplayStorage: Deleting path %s", temporary_file)
                os.unlink(temporary_file)

            del cls.replay_data[uid]
        del cls.replay_start_time[uid]

    @classmethod
    def get_replay_start_time(cls, uid: int):
        return cls.replay_start_time.get(uid, int(time()))
