import pytest
import struct

from tests.replays import example_replay
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


# FIXME - this header has been generated *BY* the function being tested.
# Make sure to replace this with independently generated data!
EXPECTED_EXAMPLE_HEADER = {
    'armies': {
        0: {
            'AIPersonality': '',
            'ArmyColor': 2.0,
            'ArmyName': 'ARMY_1',
            'BadMap': True,
            'Civilian': True,
            'Country': 'pl',
            'DEV': 97.84439849853516,
            'Faction': 3.0,
            'Human': False,
            'MEAN': 1817.47998046875,
            'NG': 1633.0,
            'ObserverListIndex': -1.0,
            'OwnerID': '6579',
            'PL': 1500.0,
            'PlayerClan': 'SNF',
            'PlayerColor': 2.0,
            'PlayerName': 'MazorNoob',
            'Ready': False,
            'StartSpot': 1.0,
            'Team': 2.0},
        1: {
            'AIPersonality': '',
            'ArmyColor': 1.0,
            'ArmyName': 'ARMY_2',
            'BadMap': True,
            'Civilian': True,
            'Country': 'cz',
            'DEV': 153.16900634765625,
            'Faction': 3.0,
            'Human': False,
            'MEAN': 1282.280029296875,
            'NG': 45.0,
            'ObserverListIndex': -1.0,
            'OwnerID': '191414',
            'PL': 800.0,
            'PlayerClan': '',
            'PlayerColor': 1.0,
            'PlayerName': 'dragonite',
            'Ready': False,
            'StartSpot': 2.0,
            'Team': 3.0
        }
    },
    'cheats_enabled': 0,
    'map_name': '/maps/SCMP_016/SCMP_016.scmap',
    'mods': {},
    'random_seed': 65637294,
    'remaining_timeouts': {
        'MazorNoob': 3,
        'dragonite': 3
    },
    'replay_version': 'Replay v1.9',
    'scenario': {
        'Configurations': {
            'standard': {
                'customprops': {},
                'teams': {
                    1.0: {
                        'armies': {
                            1.0: 'ARMY_1',
                            2.0: 'ARMY_2'
                        },
                        'name': 'FFA'
                    }
                }
            }
        },
        'Options': {
            'AllowObservers': False,
            'AutoTeams': 'tvsb',
            'BuildMult': '2.0',
            'CheatMult': '2.0',
            'CheatsEnabled': 'false',
            'CivilianAlliance': 'enemy',
            'ClanTags': {
                'MazorNoob': 'SNF',
                'dragonite': ''
            },
            'FogOfWar': 'explored',
            'GameSpeed': 'normal',
            'LandExpansionsAllowed': '5',
            'NavalExpansionsAllowed': '4',
            'NoRushOption': 'Off',
            'OmniCheat': 'on',
            'PrebuiltUnits': 'Off',
            'Quality': 35.875,
            'RandomMap': 'Off',
            'Ratings': {
                'MazorNoob': 1500.0,
                'dragonite': 800.0
            },
            'RevealCivilians': 'Yes',
            'ScenarioFile': '/maps/scmp_016/scmp_016_scenario.lua',
            'Score': 'no',
            'Share': 'ShareUntilDeath',
            'ShareUnitCap': 'none',
            'TMLRandom': '0',
            'TeamLock': 'locked',
            'TeamSpawn': 'fixed',
            'Timeouts': '3',
            'UnitCap': '1000',
            'Victory': 'demoralization'
        },
        'description': ('<LOC SCMP_016_Description>Canis River is known '
                        'for two things. The first is the river, which is '
                        'home to mineral deposits, especially gold. The '
                        "second is that it's ideal for a one-on-one "
                        'fight.'),
        'map': '/maps/SCMP_016/SCMP_016.scmap',
        'name': 'Canis River',
        'norushoffsetX_ARMY_1': 5.0,
        'norushoffsetX_ARMY_2': -25.0,
        'norushoffsetY_ARMY_1': -20.0,
        'norushoffsetY_ARMY_2': 0.0,
        'norushradius': 75.0,
        'preview': '',
        'save': '/maps/SCMP_016/SCMP_016_save.lua',
        'script': '/maps/SCMP_016/SCMP_016_script.lua',
        'size': {
            1.0: 256.0,
            2.0: 256.0
        },
        'starts': False,
        'type': 'skirmish'
    },
    'version': 'Supreme Commander v1.50.3696'
}


def test_load_example_header():
    replay_data = example_replay.data
    with pytest.raises(StopIteration) as v:
        run_cor(header.read_header, replay_data)
    assert v.value.value == EXPECTED_EXAMPLE_HEADER


@pytest.mark.asyncio
async def test_replayheader_coroutine(mock_connections):
    conn = mock_connections(None, None)
    conn.set_mock_read_data(example_replay.data)
    head, leftovers = await header.ReplayHeader.from_connection(conn)
    assert head.header == EXPECTED_EXAMPLE_HEADER
    assert example_replay.data.startswith(head.data + leftovers)
