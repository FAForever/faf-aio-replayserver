import pytest
from tests import timeout

from replayserver.receive.stream import ConnectionReplayStream
from replayserver.errors import MalformedDataError


@pytest.fixture
def mock_header_reader(mocker):
    class R:
        def done():
            pass

        def send():
            pass

        def result():
            pass

    return mocker.Mock(spec=R)


@pytest.mark.asyncio
@timeout(1)
async def test_replay_stream_read_header(mock_header_reader, mock_connections):
    mock_conn = mock_connections(None, None)
    stream = ConnectionReplayStream(mock_header_reader, mock_conn)

    mock_conn.read.side_effect = [b"Lorem ", b"ipsum ", b"dolor"]

    read_data = b""

    def mock_send(data):
        nonlocal read_data
        read_data += data

    def enough_data():
        return read_data.startswith(b"Lorem ips")

    mock_header_reader.send.side_effect = mock_send
    mock_header_reader.done.side_effect = enough_data
    mock_header_reader.result.return_value = (b"Lorem ips", b"um ")

    await stream.read_header()
    assert mock_conn.read.await_count == 2
    assert stream.header == b"Lorem ips"
    assert stream.data == b"um "


@pytest.mark.asyncio
@timeout(1)
async def test_replay_stream_invalid_header(
        mock_header_reader, mock_connections):
    mock_conn = mock_connections(None, None)
    stream = ConnectionReplayStream(mock_header_reader, mock_conn)

    mock_conn.read.side_effect = [b"Lorem ", b"ipsum ", b"dolor"]

    read_data = b""

    def mock_send(data):
        nonlocal read_data
        read_data += data
        if read_data.startswith(b"Lorem ips"):
            raise ValueError

    mock_header_reader.send.side_effect = mock_send
    mock_header_reader.done.return_value = False

    with pytest.raises(MalformedDataError):
        await stream.read_header()


@pytest.mark.asyncio
@timeout(1)
async def test_replay_stream_too_short_header(
        mock_header_reader, mock_connections):
    mock_conn = mock_connections(None, None)
    stream = ConnectionReplayStream(mock_header_reader, mock_conn)

    mock_conn.read.side_effect = [b"Lorem ", b"ip", b""]

    read_data = b""

    def mock_send(data):
        nonlocal read_data
        read_data += data

    def enough_data():
        return read_data.startswith(b"Lorem ips")

    mock_header_reader.send.side_effect = mock_send
    mock_header_reader.done.side_effect = enough_data

    with pytest.raises(MalformedDataError):
        await stream.read_header()
