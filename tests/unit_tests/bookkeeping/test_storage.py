import pytest
import asynctest
import datetime
import os
import stat

from tests.replays import example_replay, unpack_replay
from replayserver.bookkeeping.storage import ReplayFilePaths, ReplaySaver
from replayserver.errors import BookkeepingError


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


def test_replay_paths_second_access_not_allowed(tmpdir):
    # We should not allow getting the same path twice to avoid overwriting or
    # corrupting the existing replay
    paths = ReplayFilePaths(str(tmpdir))
    paths.get(1123456789)
    with pytest.raises(BookkeepingError):
        paths.get(1123456789)


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


def_teams_in_game = {1: ["user1"], 2: ["user2"]}
def_game_stats = {
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
def_mod_versions = {'1': 1}


@pytest.fixture
def standard_saver_args(mock_replay_paths, mock_database_queries, tmpdir):
    rfile = str(tmpdir.join("replay"))
    open(rfile, "a").close()
    mock_replay_paths.get.return_value = rfile
    mock_database_queries.get_teams_in_game.return_value = def_teams_in_game
    mock_database_queries.get_game_stats.return_value = def_game_stats
    mock_database_queries.get_mod_versions.return_value = def_mod_versions
    return mock_replay_paths, mock_database_queries


def set_example_stream_data(outside_source_stream, mock_replay_headers):
    mock_header = mock_replay_headers(example_replay)
    outside_source_stream.set_header(mock_header)
    outside_source_stream.feed_data(b"bar")
    outside_source_stream.finish()


@pytest.mark.asyncio
async def test_replay_saver_save_replay(standard_saver_args,
                                        mock_replay_headers,
                                        outside_source_stream, tmpdir):
    set_example_stream_data(outside_source_stream, mock_replay_headers)

    saver = ReplaySaver(*standard_saver_args)
    await saver.save_replay(1111, outside_source_stream)

    rfile = str(tmpdir.join("replay"))
    head, rep = unpack_replay(open(rfile, "rb").read())
    assert type(head) is dict
    assert head['uid'] == 1111
    assert head['teams'] == {"1": ["user1"], "2": ["user2"]}
    assert head['featured_mod_versions'] == def_mod_versions
    for item in def_game_stats:
        assert head[item] == def_game_stats[item]
    assert rep == example_replay.header_data + b"bar"


@pytest.mark.asyncio
async def test_replay_saver_no_header(standard_saver_args,
                                      outside_source_stream, tmpdir):
    outside_source_stream.finish()
    saver = ReplaySaver(*standard_saver_args)
    with pytest.raises(BookkeepingError):
        await saver.save_replay(1111, outside_source_stream)


@pytest.mark.asyncio
async def test_replay_saver_readonly_file(standard_saver_args,
                                          mock_replay_headers,
                                          outside_source_stream, tmpdir):
    set_example_stream_data(outside_source_stream, mock_replay_headers)

    rfile = str(tmpdir.join("replay"))
    os.chmod(rfile, stat.S_IRUSR)

    saver = ReplaySaver(*standard_saver_args)
    with pytest.raises(BookkeepingError):
        await saver.save_replay(1111, outside_source_stream)


@pytest.mark.asyncio
async def test_replay_saver_null_team(standard_saver_args,
                                      mock_replay_headers,
                                      outside_source_stream, tmpdir):
    set_example_stream_data(outside_source_stream, mock_replay_headers)
    mock_queries = standard_saver_args[1]
    mock_queries.get_teams_in_game.return_value = {
        1: ["user1"], 2: ["user2"], None: ["SomeGuy"]
    }

    saver = ReplaySaver(*standard_saver_args)
    await saver.save_replay(1111, outside_source_stream)
    rfile = str(tmpdir.join("replay"))
    head, rep = unpack_replay(open(rfile, "rb").read())
    assert head["teams"]["null"] == ["SomeGuy"]
