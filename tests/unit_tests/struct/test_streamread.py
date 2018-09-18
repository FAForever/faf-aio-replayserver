import pytest
from replayserver.struct import streamread


def test_generator_data():
    gen = streamread.GeneratorData()
    cor = gen.more()
    cor.send(None)
    with pytest.raises(StopIteration) as v:
        cor.send(b"foobar")
        assert v.value == 6

    assert gen.data == b"foobar"
    assert gen.position == 0

    data = gen.take(4)
    assert data == b"foob"
    assert gen.position == 4
    assert gen.data == b"foobar"

    data = gen.take(2)
    assert data == b"ar"
    assert gen.position == 6
    assert gen.data == b"foobar"


def test_generator_maxlen():
    def do_send(gen, data):
        cor = gen.more()
        cor.send(None)
        try:
            cor.send(data)
        except StopIteration:
            pass

    gen = streamread.GeneratorData(maxlen=8)
    do_send(gen, b"12345678")
    with pytest.raises(ValueError):
        do_send(gen, b"12345678")

    gen = streamread.GeneratorData(maxlen=6)
    do_send(gen, b"12345678")
    with pytest.raises(ValueError):
        do_send(gen, b"12345678")

    gen = streamread.GeneratorData(maxlen=6)
    do_send(gen, b"12345678")
    gen.take(6)
    gen.take(0)
    with pytest.raises(ValueError):
        gen.take(2)

    gen = streamread.GeneratorData(maxlen=6)
    do_send(gen, b"12345678")
    gen.take(5)
    with pytest.raises(ValueError):
        gen.take(2)


def test_generator_wrapper():
    def testing_coro():
        stuff = yield
        assert stuff == 17
        more_stuff = yield
        assert more_stuff == 42
        return 66

    wrapper = streamread.GeneratorWrapper(testing_coro())
    assert not wrapper.done()
    wrapper.send(17)
    assert not wrapper.done()
    wrapper.send(42)
    assert wrapper.done
    assert wrapper.result() == 66


def test_generator_wrapper_exceptions():
    def throwing_coro():
        yield
        yield
        raise ValueError

    wrapper = streamread.GeneratorWrapper(throwing_coro())
    wrapper.send(17)
    with pytest.raises(ValueError):
        wrapper.send(42)


# From here on we'll assume Generator{Data,Wrapper} work. A tiny violation of
# unit testing rules, but oh well.
def test_read_exactly():
    gen = streamread.GeneratorData()
    gen.data = b"abcdefgh"
    gen.position = 4
    cor = streamread.read_exactly(gen, 3)
    with pytest.raises(StopIteration) as v:
        cor.send(None)
        assert v.value == b"efg"

    gen.data = b"1234"
    gen.position = 3
    cor = streamread.read_exactly(gen, 3)
    cor.send(None)
    cor.send(b"5")
    with pytest.raises(StopIteration) as v:
        cor.send(b"678")
        assert v.value == "456"
