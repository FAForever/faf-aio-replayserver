import os
from collections import Iterator

import pytest
from replay_parser.replay import continuous_parse

from replay_server.constants import TERMINATOR

__all__ = (
    'replay_id', 'bad_replay', 'replay_filename', 'get_replay_command', 'put_replay_command', 'replay_data',
    'replays_data', 'replay_data_different', 'replay_data_different2', 'streamed_replay_data'
)


@pytest.fixture
def replay_id():
    return 8468507


@pytest.fixture(
    params=[b'', b'Z/zzz/7640/', b"abrakadabra", b"/1112/name/lalala", b"/1112//lalala", b"///"]
)
def bad_replay(request):
    return request.param + TERMINATOR + TERMINATOR


@pytest.fixture
def replay_filename():
    return "8468507.scfareplay"


@pytest.fixture
def get_replay_command(replay_id):
    return b'G/' + bytes("{}".format(replay_id), encoding='ascii') + b'/' + TERMINATOR


@pytest.fixture
def put_replay_command(replay_id):
    return b'P/' + bytes("{}".format(replay_id), encoding='ascii') + b'/' + TERMINATOR


@pytest.fixture
def replay_data(replay_filename):
    replay_path = os.path.abspath(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), "replays", replay_filename)
    )
    with open(replay_path, "rb") as f:
        return f.read()


@pytest.yield_fixture
def replays_data():
    def get_replay_data():
        for file_name in ["8246215-dragonite.scfareplay", "8246215-MazorNoob.scfareplay", "8246215.scfareplay"]:
            replay_path = os.path.abspath(
                os.path.join(os.path.dirname(os.path.realpath(__file__)), "replays", file_name)
            )
            with open(replay_path, "rb") as f:
                data = f.read()
                yield data

    return get_replay_data


@pytest.fixture
def replay_data_different(replay_data):
    replay_copy = bytearray(replay_data[:])
    # let's add some "difference" at second third part
    replay_copy[-1 * int(len(replay_copy) // 3)] = 126
    return replay_copy


@pytest.fixture
def replay_data_different2(replay_data):
    replay_copy = bytearray(replay_data[:])
    # let's add some "difference" at second third part
    replay_copy[-1 * int(len(replay_copy) // 3)] = 125
    return replay_copy


@pytest.yield_fixture
def streamed_replay_data(replay_data):
    """
    Continuous stream command by command.
    First yield is header, then yields replay content per command
    """
    # yield header
    def iterator():
        parser: Iterator = continuous_parse(
            replay_data,
            parse_header=True,
            parse_commands={-1}
        )
        header = next(parser)
        yield replay_data[:header['body_offset']]

        # yield body
        for tick, command_type, data in parser:
            yield data

    return iterator()
