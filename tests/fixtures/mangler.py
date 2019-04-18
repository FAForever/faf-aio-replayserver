import pytest


class TestMangler:
    def mangle(self, data):
        return data

    def drain(self):
        return b""


@pytest.fixture
def id_mangler():
    return TestMangler


@pytest.fixture
def mock_mangler(mocker):
    return mocker.Mock(spec=TestMangler)
