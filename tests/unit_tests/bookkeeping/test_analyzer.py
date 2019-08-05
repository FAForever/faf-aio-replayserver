import struct

import pytest
from fafreplay import commands
from replayserver.bookkeeping.analyzer import ReplayAnalyzer
from replayserver.errors import BookkeepingError


@pytest.fixture
def analyzer():
    return ReplayAnalyzer()


@pytest.fixture
def replay_body_data():
    data = bytearray()
    data += struct.pack("B", commands.Advance)
    data += struct.pack("<H", 4 + 3)   # Size of the entire command payload
    data += struct.pack("<i", 20)      # Number of ticks

    data += struct.pack("B", commands.EndGame)
    data += struct.pack("<H", 0 + 3)
    return data


@pytest.fixture
def corrupt_body_data():
    return bytearray(10)


def test_get_replay_ticks(analyzer, replay_body_data):
    ticks = analyzer.get_replay_ticks(replay_body_data)

    assert ticks == 20


def test_parse_corrupt(analyzer, corrupt_body_data):
    with pytest.raises(BookkeepingError):
        analyzer.get_replay_ticks(corrupt_body_data)
