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
