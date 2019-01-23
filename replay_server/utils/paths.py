import os
from uuid import uuid4

from replay_server import constants

__all__ = ('get_temp_replay_path', 'get_replay_path')


def _get_dir_path(base_dir: str, uid: int, suffix: str, unique: bool):
    """
    Prepares file path, and returns new file name.
    """
    dir_size = 100
    depth = 5
    dir_parts = [base_dir]
    while depth > 1:
        depth -= 1
        dir_parts.append(str((uid // (dir_size ** depth)) % dir_size))

    dir_path = os.path.join(*dir_parts)
    os.makedirs(dir_path, exist_ok=True)

    file_name = str(uid)
    if unique:
        file_name = "{}-{}".format(file_name, uuid4())

    return "{}.{}".format(os.path.join(*[dir_path, file_name]), suffix)


def get_temp_replay_path(uid: int):
    """
    Returns path to temporary directory
    """
    return _get_dir_path(constants.TEMPORARY_DIR, uid, "scfareplay", True)


def get_replay_path(uid: int):
    """
    Returns path to replay directory
    """
    return _get_dir_path(constants.REPLAY_DIR, uid, "fafreplay", False)
