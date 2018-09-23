import pytest
import asynctest
from replayserver.receive.stream import OutsideSourceReplayStream


# Technically we're breaking the unittest rule of not involving other units.
# In practice for testing purposes we'd basically have to reimplement
# OutsideSourceReplayStream, so let's just use it - at least it's unit-tested
# itself.
#
# Yes, OutsideSourceReplayStream can change in the future so it's not useful
# for unit testing anymore. We'll just haul the current implementation here
# when that happens.
@pytest.fixture
def outside_source_stream():
    return asynctest.Mock(wraps=OutsideSourceReplayStream())
