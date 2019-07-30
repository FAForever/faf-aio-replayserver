from replayserver.receive.memprefix import memprefix


def test_memprefix_sanity_check():
    b1 = b"1" * 40000 + b"0" * 20000
    b2 = b"1" * 39876 + b"2" + b"1" * 133 + b"0" * 20000
    assert memprefix(b1, b2) == 39876

    b1 = b"1" * 10
    b2 = b"1" * 14
    assert memprefix(b1, b2) == 10

    b1 = b"1" * 5 + b"2" * 2
    b2 = b"1" * 7
    assert memprefix(b1, b2, end=4) == 4
    assert memprefix(b1, b2, end=5) == 5
    assert memprefix(b1, b2, end=6) == 5
