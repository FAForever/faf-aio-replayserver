import pytest
from tests import timeout
from replayserver.send.sender import Sender
from replayserver.errors import CannotAcceptConnectionError
from replayserver.server.connection import Connection


@pytest.mark.asyncio
@timeout(0.1)
async def test_sender_not_accepting_after_stream_ends(mock_connections,
                                                      outside_source_stream):
    connection = mock_connections(None, None)
    connection.type = Connection.Type.READER
    connection.uid = 1

    sender = Sender(outside_source_stream)
    outside_source_stream.finish()
    with pytest.raises(CannotAcceptConnectionError):
        await sender.handle_connection(connection)
