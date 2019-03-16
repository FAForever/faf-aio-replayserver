from replayserver.receive.offlinemerger import memprefix, DataMerger, \
    OfflineReplayMerger


def test_memprefix_sanity_check():
    b1 = b"1" * 40000 + b"0" * 20000
    b2 = b"1" * 39876 + b"2" + b"1" * 133 + b"0" * 20000
    assert memprefix(b1, b2) == 39876

    b1 = b"1" * 10
    b2 = b"1" * 14
    assert memprefix(b1, b2) == 10

    b1 = b"1" * 5 + b"2" * 5
    b2 = b"0" * 7 + b"2" * 3
    assert memprefix(b1, b2, start=7) == 10


def test_datamerger_sanity_check():
    merger = DataMerger()
    datas = [
        b"aaaaaaaabbbb",
        b"aaaaaaaacccc",
        b"aaaaaaaa",
        b"aaaaaaaabb"]
    for i, data in enumerate(datas):
        merger.add_data(data, i)

    best = merger.get_best_data()
    assert best == 0


def test_datamerger_same_data():
    merger = DataMerger()
    datas = [
        b"aaaaaaaabbbb",
        b"aaaaaaaabbbb",
        b"aaaaaaaacccc"]
    for i, data in enumerate(datas):
        merger.add_data(data, i)

    best = merger.get_best_data()
    assert best in [0, 1]


def test_datamerger_improving_strings():
    merger = DataMerger()
    datas = [
        b"aabb",
        b"aabb",
        b"aabbb",
        b"acccc",
        b"acccc",
        b"acccc",
        b"acccc"]
    for i, data in enumerate(datas):
        merger.add_data(data, i)

    best = merger.get_best_data()
    assert best in range(3, 7)


def test_datamerger_no_data():
    merger = DataMerger()
    assert merger.get_best_data() is None


def test_replaymerger_no_replays():
    # I guess it's not a unit test? But the only dep of merger is DataMerger,
    # so I don't care much
    merger = OfflineReplayMerger.build()
    assert merger.get_best_replay() is None
