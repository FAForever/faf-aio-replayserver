import struct
from enum import Enum

from replayserver.struct.streamread import GeneratorData, GeneratorWrapper, \
    read_exactly, read_until


class LuaType(Enum):
    NUMBER = 0
    STRING = 1
    NIL = 2
    BOOL = 3
    LUA = 4
    LUA_END = 5


# Expects to receive exactly one value to unpack in format.
def read_value(gen, fmt, size):
    fmt = "<" + fmt     # All data is little-endian.
    data = yield from read_exactly(gen, size)
    try:
        return struct.unpack(fmt, data)[0]
    except struct.error:
        raise ValueError


def read_string(gen):
    data = yield from read_until(gen, b'\0')
    try:
        return data[:-1].decode()
    except UnicodeDecodeError:
        raise ValueError


def read_lua_type(gen):
    type_ = yield from read_value(gen, "B", 1)
    return LuaType(type_)     # can raise ValueError


def read_lua_value(gen, can_be_lua_end=False):
    type_ = yield from read_lua_type(gen)

    if type_ == LuaType.NUMBER:
        return (yield from read_value(gen, "f", 4))
    elif type_ == LuaType.STRING:
        return (yield from read_string(gen))
    elif type_ == LuaType.NIL:
        return None
    elif type_ == LuaType.BOOL:
        ret = (yield from read_value(gen, "B", 1))
        return ret == 0     # Not a typo
    elif type_ == LuaType.LUA_END:
        if can_be_lua_end:
            return LuaType.LUA_END
        else:
            raise ValueError
    elif type_ == LuaType.LUA:
        result = {}
        while True:
            key = yield from read_lua_value(gen, True)
            if key == LuaType.LUA_END:
                return result
            value = yield from read_lua_value(gen)
            result[key] = value


def read_header(gen):
    result = {}
    result["version"] = yield from read_string(gen)
    yield from read_exactly(gen, 3)     # skip

    replay_version_and_map = yield from read_string(gen)
    # can raise ValueError
    replay_version, map_name = replay_version_and_map.split(b"\n", 2)
    result["replay_version"] = replay_version
    result["map_name"] = map_name
    yield from read_exactly(gen, 4)     # skip

    yield from read_value(gen, "I", 4)  # Mod (data?) size
    result["mods"] = yield from read_lua_value(gen)

    yield from read_value(gen, "I", 4)  # Scenario (data?) size
    result["scenario"] = yield from read_lua_value(gen)

    player_count = yield from read_value(gen, "b", 1)
    timeouts = {}
    for i in range(player_count):
        name = yield from read_string(gen)
        number = yield from read_value(gen, "I", 4)
        timeouts[name] = number
    result["remaining_timeouts"] = timeouts

    result["cheats_enabled"] = yield from read_value(gen, "B", 1)

    army_count = yield from read_value(gen, "B", 1)
    armies = {}
    for i in range(army_count):
        yield from read_value(gen, "I", 4)  # Army (data?) size
        army = yield from read_lua_value(gen)
        player_id = yield from read_value(gen, "B", 1)
        armies[player_id] = army
        if player_id != 255:
            yield from read_exactly(1)      # Unknown skip
    result["armies"] = armies

    result["random_seed"] = yield from read_value(gen, "I", 4)
    return result


class ReplayHeader:
    MAXLEN = 32 * 1024  # 32 kbytes ought to be enough for everyone

    def __init__(self, data, header):
        self.data = data
        self.header = header

    @classmethod
    def generate(cls, maxlen):
        gen = GeneratorData(maxlen)
        header = yield from read_header(gen)
        data = gen.data[:gen.position]
        leftovers = gen.data[gen.position:]
        return (cls(data, header), leftovers)

    @classmethod
    def generator(cls):
        return GeneratorWrapper(cls.generate)
