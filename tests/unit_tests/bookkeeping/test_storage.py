import pytest
import asynctest
import datetime

from replayserver.bookkeeping.storage import ReplayFilePaths, ReplaySaver
from tests.replays import example_replay, unpack_replay


def test_replay_paths(tmpdir):
    paths = ReplayFilePaths(str(tmpdir))
    rpath = paths.get(1123456789)
    expected = tmpdir.join("11", "23", "45", "67",
                           "1123456789.fafreplay")
    assert rpath == str(expected)
    assert expected.exists()


def test_replay_paths_odd_ids(tmpdir):
    paths = ReplayFilePaths(str(tmpdir))

    rpath = paths.get(12345)
    assert rpath == str(tmpdir.join("0", "0", "1", "23",
                                    "12345.fafreplay"))

    rpath = paths.get(0)
    assert rpath == str(tmpdir.join("0", "0", "0", "0",
                                    "0.fafreplay"))

    rpath = paths.get(101010101)
    assert rpath == str(tmpdir.join("1", "1", "1", "1",
                                    "101010101.fafreplay"))

    # This is where folder structure breaks a bit :)
    # But I imagine same legacy code is used everywhere, so let's keep that
    # broken behaviour.
    # We didn't even break 8 digits yet anyway.
    rpath = paths.get(111122223333)
    assert rpath == str(tmpdir.join("11", "22", "22", "33",
                                    "111122223333.fafreplay"))


def test_replay_paths_same_folder(tmpdir):
    paths = ReplayFilePaths(str(tmpdir))
    paths.get(11111111)
    paths.get(11111112)
    assert tmpdir.join("0", "11", "11", "11", "11111111.fafreplay").exists()
    assert tmpdir.join("0", "11", "11", "11", "11111112.fafreplay").exists()


@pytest.fixture
def mock_replay_paths():
    return asynctest.Mock(spec=['get'])


@pytest.fixture
def mock_database_queries():
    class Q:
        async def get_teams_in_game():
            pass

        async def get_game_stats():
            pass

        async def get_mod_versions():
            pass

    return asynctest.Mock(spec=Q)


@pytest.mark.asyncio
async def test_replay_saver_save_replay(mock_replay_paths,
                                        mock_database_queries,
                                        mock_replay_headers,
                                        outside_source_stream, tmpdir):
    mock_header = mock_replay_headers(example_replay)
    outside_source_stream.set_header(mock_header)
    outside_source_stream.feed_data(b"bar")
    outside_source_stream.finish()

    rfile = str(tmpdir.join("replay"))
    open(rfile, "a").close()
    mock_replay_paths.get.return_value = rfile

    mock_database_queries.get_teams_in_game.return_value = {
        1: ["user1"],
        2: ["user2"],
    }
    mock_database_queries.get_game_stats.return_value = {
        'featured_mod': 'faf',
        'game_type': '0',
        'recorder': 'user1',
        'host': 'user1',
        'launched_at': datetime.datetime(2001, 1, 1, 0, 0).timestamp(),
        'game_end': datetime.datetime(2001, 1, 2, 0, 0).timestamp(),
        'title': 'Name of the game',
        'mapname': 'scmp_1',
        'map_file_path': 'maps/scmp_1.zip',
        'num_players': 2
    }
    mock_database_queries.get_mod_versions.return_value = {
        '1': 1,
    }

    saver = ReplaySaver(mock_replay_paths, mock_database_queries)
    await saver.save_replay(1111, outside_source_stream)

    head, rep = unpack_replay(open(rfile, "rb").read())
    assert type(head) is dict   # TODO - test contents
    assert rep == example_replay.header_data + b"bar"
