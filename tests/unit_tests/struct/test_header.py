import pytest
import struct

from replayserver.struct.streamread import GeneratorData
from replayserver.struct import header


# We assume GeneratorData works, for sake of easier testing.
# We have unit tests for it wnyway.
def run_cor(cor, data, *args):
    gen = GeneratorData()
    gen.data = data
    cor = cor(gen, *args)
    cor.send(None)


def test_read_value_endianness():
    with pytest.raises(StopIteration) as v:
        run_cor(header.read_value, b"\1\0\0\0", "I", 4)
    assert v.value.value == 1


def test_read_value_struct_error():
    with pytest.raises(ValueError):
        run_cor(header.read_value, b"\0\0\0\0\0\0", "f", 2)


def test_read_string():
    with pytest.raises(StopIteration) as v:
        run_cor(header.read_string, b"aaaaa\0")
    assert v.value.value == "aaaaa"


def test_read_string_invalid_unicode():
    # Lonely start character is invalid unicode
    data = b"aaaa\xc0 \0"
    with pytest.raises(ValueError):
        run_cor(header.read_string, data)


def test_read_lua_type():
    with pytest.raises(StopIteration) as v:
        run_cor(header.read_lua_type, b"\0")
    assert v.value.value == header.LuaType.NUMBER

    with pytest.raises(ValueError):
        run_cor(header.read_lua_type, b"\x42")


def test_lua_nil_value():
    with pytest.raises(StopIteration) as v:
        run_cor(header.read_lua_value, b"\2")
    assert v.value.value is None


def test_lua_number_value():
    number = 2.375  # Nice representable number
    data = b"\0" + struct.pack("<f", number)
    with pytest.raises(StopIteration) as v:
        run_cor(header.read_lua_value, data)
    assert v.value.value == number


def test_lua_bool_value():
    with pytest.raises(StopIteration) as v:
        run_cor(header.read_lua_value, b"\3\x17")
    assert v.value.value is False   # Not a typo

    with pytest.raises(StopIteration) as v:
        run_cor(header.read_lua_value, b"\3\0")
    assert v.value.value is True   # Not a typo


def test_lua_string_value():
    data = b"\1aaaaa\0"
    with pytest.raises(StopIteration) as v:
        run_cor(header.read_lua_value, data)
    assert v.value.value == "aaaaa"

    # Check invalid unicode just in case
    data = b"\1aaaa\xc0 \0"
    with pytest.raises(ValueError) as v:
        run_cor(header.read_lua_value, data)


def test_lua_dict_value():
    data = b"\4\1a\0\1b\0\5"
    with pytest.raises(StopIteration) as v:
        run_cor(header.read_lua_value, data)
    assert v.value.value == {"a": "b"}

    with pytest.raises(StopIteration) as v:
        run_cor(header.read_lua_value, b"\4\5")
    assert v.value.value == {}


def test_lua_nested_dict():
    data = b"\4\2\4\1a\0\4\5\5\5"
    with pytest.raises(StopIteration) as v:
        run_cor(header.read_lua_value, data)
    assert v.value.value == {None: {"a": {}}}


def test_lua_dict_as_key():
    with pytest.raises(ValueError):
        run_cor(header.read_lua_value, b"\4\4\5\2\5")


def test_lua_unpaired_dict_end():
    data = b"\5\4"
    with pytest.raises(ValueError):
        run_cor(header.read_lua_value, data)


def test_lua_dict_odd_item_number():
    data = b"\4\1a\0\1b\0\1c\0\5"
    with pytest.raises(ValueError):
        run_cor(header.read_lua_value, data)
