from replayserver.bookkeeping.storage import ReplayFilePaths


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
