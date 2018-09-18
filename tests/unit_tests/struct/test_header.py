import pytest

from replayserver.struct.streamread import GeneratorData
from replayserver.struct import header


# We assume GeneratorData works, for sake of easier testing.
# We have unit tests for it wnyway.
def test_read_value_endianness():
    gen = GeneratorData()
    gen.data = b"\1\0\0\0"
    cor = header.read_value(gen, "I", 4)
    with pytest.raises(StopIteration) as v:
        cor.send(None)
    assert v.value.value == 1


def test_read_value_struct_error():
    gen = GeneratorData()
    gen.data = b"\0\0\0\0\0\0"
    cor = header.read_value(gen, "f", 2)
    with pytest.raises(ValueError):
        cor.send(None)


def test_read_string():
    gen = GeneratorData()
    gen.data = b"aaaaa\0"
    cor = header.read_string(gen)
    with pytest.raises(StopIteration) as v:
        cor.send(None)
    assert v.value.value == "aaaaa"


def test_read_string_invalid_unicode():
    gen = GeneratorData()
    # Lonely start character is invalid unicode
    gen.data = b"aaaa\xc0 \0"
    cor = header.read_string(gen)
    with pytest.raises(ValueError):
        cor.send(None)


def test_read_lua_type():
    gen = GeneratorData()
    gen.data = b"\0"
    cor = header.read_lua_type(gen)
    with pytest.raises(StopIteration) as v:
        cor.send(None)
        assert v.value.value == header.LuaType.NUMBER

    gen = GeneratorData()
    gen.data = b"\x42"
    cor = header.read_lua_type(gen)
    with pytest.raises(ValueError):
        cor.send(None)
