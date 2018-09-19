import pytest
import struct

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


def test_lua_nil_value():
    gen = GeneratorData()
    gen.data = b"\2"
    cor = header.read_lua_value(gen)
    with pytest.raises(StopIteration) as v:
        cor.send(None)
    assert v.value.value is None


def test_lua_number_value():
    gen = GeneratorData()
    number = 2.375  # Nice representable number
    gen.data = b"\0" + struct.pack("<f", number)
    cor = header.read_lua_value(gen)
    with pytest.raises(StopIteration) as v:
        cor.send(None)
    assert v.value.value == number


def test_lua_bool_value():
    gen = GeneratorData()
    gen.data = b"\3\x17"
    cor = header.read_lua_value(gen)
    with pytest.raises(StopIteration) as v:
        cor.send(None)
    assert v.value.value is False   # Not a typo

    gen = GeneratorData()
    gen.data = b"\3\0"
    cor = header.read_lua_value(gen)
    with pytest.raises(StopIteration) as v:
        cor.send(None)
    assert v.value.value is True   # Not a typo


def test_lua_string_value():
    gen = GeneratorData()
    gen.data = b"\1aaaaa\0"
    cor = header.read_lua_value(gen)
    with pytest.raises(StopIteration) as v:
        cor.send(None)
    assert v.value.value == "aaaaa"

    # Check invalid unicode just in case
    gen = GeneratorData()
    gen.data = b"\1aaaa\xc0 \0"
    cor = header.read_lua_value(gen)
    with pytest.raises(ValueError) as v:
        cor.send(None)


def test_lua_dict_value():
    gen = GeneratorData()
    gen.data = b"\4\1a\0\1b\0\5"
    cor = header.read_lua_value(gen)
    with pytest.raises(StopIteration) as v:
        cor.send(None)
    assert v.value.value == {"a": "b"}

    gen = GeneratorData()
    gen.data = b"\4\5"
    cor = header.read_lua_value(gen)
    with pytest.raises(StopIteration) as v:
        cor.send(None)
    assert v.value.value == {}

    gen = GeneratorData()
    gen.data = b"\4\2\4\1a\0\1b\0\5\5"
    cor = header.read_lua_value(gen)
    with pytest.raises(StopIteration) as v:
        cor.send(None)
    assert v.value.value == {None: {"a": "b"}}
