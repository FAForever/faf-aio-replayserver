from io import FileIO
from typing import Dict

__all__ = ('ReplayStorage',)


class ReplayStorage:
    """
    Handle replay data
    """
    # List of byte arrays, where data are stored.
    replay_data: Dict[int, Dict[str, FileIO]] = {}

    @classmethod
    def get_replays(cls, uid: int):
        return cls.replay_data[uid]

    @classmethod
    def has_replays(cls, uid: int):
        return uid in cls.replay_data

    @classmethod
    def set_replay(cls, uid: int, file_path: str, file_: FileIO):
        cls.replay_data.setdefault(uid, {})[file_path] = file_

    @classmethod
    def remove_replay_data(cls, uid: int):
        if uid in cls.replay_data:
            del cls.replay_data[uid]
