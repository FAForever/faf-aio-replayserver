from replay_server import constants
from replay_server.utils.paths import get_temp_replay_path, get_replay_path
from replay_server.replay_parser.replay_parser import parser
from replay_server.utils.greatest_common_replay import get_replay


def test_file_path_save(replay_id):
    """
    Check file name distribution on disc
    """
    tmp_path = get_temp_replay_path(replay_id)
    assert constants.TEMPORARY_DIR in tmp_path
    assert str(replay_id) in tmp_path

    replay_path = get_replay_path(replay_id)
    assert constants.REPLAY_DIR in replay_path
    assert str(replay_id) in replay_path


def test_replay_structure(replay_data):
    """
    Checks if replay some data after parse.
    """
    result = parser.parse(replay_data)
    assert result['body_offset'] > 0
    assert result['header']


def test_replay_parser_header(replays_data):
    """
    If there are all different replays, there is always same header diff.
    """
    biggest_header = None
    for data in replays_data():
        header = parser.parse(data)['header']
        if not biggest_header:
            biggest_header = header
        else:
            diff = set(biggest_header.keys()) - set(header.keys())
            assert not diff
            diff = set(header.keys()) - set(biggest_header.keys())
            assert not diff


def test_get_greatest_common_replays(replay_id, replay_data, replay_data_different, replay_data_different2):
    """
    We will save more than one replay, when that happen we'll choose the longest common ones
    """
    original_file_paths = []

    # longest right one
    file_name = get_temp_replay_path(replay_id)
    with open(file_name, "wb") as f:
        f.write(replay_data)
    original_file_paths.append(file_name)

    # a little bit smaller right one, like somebody disconnected a second before
    file_name = get_temp_replay_path(replay_id)
    with open(file_name, "wb") as f:
        f.write(replay_data[:len(replay_data) - 3])
    original_file_paths.append(file_name)

    # a smaller right one, like somebody disconnected a 2 seconds before
    file_name = get_temp_replay_path(replay_id)
    with open(file_name, "wb") as f:
        f.write(replay_data[:len(replay_data) - 6])
    original_file_paths.append(file_name)

    # another "truth", biggest one
    file_name = get_temp_replay_path(replay_id)
    with open(file_name, "wb") as f:
        f.write(replay_data_different)
    original_file_paths.append(file_name)

    # a little bit smaller, like somebody disconnected a second before
    file_name = get_temp_replay_path(replay_id)
    with open(file_name, "wb") as f:
        f.write(replay_data_different[:len(replay_data_different) - 3])
    original_file_paths.append(file_name)

    # a smaller right one, like somebody disconnected a 2 seconds before
    file_name = get_temp_replay_path(replay_id)
    with open(file_name, "wb") as f:
        f.write(replay_data_different[:len(replay_data_different) - 6])
    original_file_paths.append(file_name)

    # totally different
    file_name = get_temp_replay_path(replay_id)
    with open(file_name, "wb") as f:
        f.write(replay_data_different2)
    original_file_paths.append(file_name)

    file_path = get_replay(original_file_paths)
    assert isinstance(file_path, str)
