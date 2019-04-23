from replayserver.common import ServesConnections
from replayserver.streams import DelayedReplayStream
from replayserver import config


class ReplayStreamWriter:
    def __init__(self, stream):
        self._stream = stream

    @classmethod
    def build(cls, stream):
        return cls(stream)

    async def send_to(self, connection):
        await self._write_header(connection)
        if self._stream.header is None:
            return
        await self._write_replay(connection)

    async def _write_header(self, connection):
        header = await self._stream.wait_for_header()
        if header is not None:
            await connection.write(header.data)

    async def _write_replay(self, connection):
        position = 0
        while True:
            data = await self._stream.wait_for_data(position)
            if not data:
                break
            position += len(data)
            conn_open = await connection.write(data)
            if not conn_open:
                break


class SenderConfig(config.Config):
    _options = {
        "replay_delay": {
            "parser": config.positive_float,
            "doc": ("Delay in seconds between receiving replay data and "
                    "sending it to readers. Used to prevent cheating via "
                    "playing and observing at the same time.\n\n"
                    "Note that current replay stream merging strategy relies "
                    "on having a buffer of future data in can compare "
                    "between streams! It's highly recommended to set this to "
                    "a reasonably high value (e.g. five minutes).")
        },
        "update_interval": {
            "parser": config.positive_float,
            "doc": ("Frequency, in seconds, of checking for new data to send "
                    "to listeners. This affects frequency of calling send() "
                    "of listener sockets, as the server sets high/low buffer "
                    "water marks to 0 in order to prevent unwanted latency. "
                    "Setting this value higher might improve performance.")
        }
    }


class Sender(ServesConnections):
    def __init__(self, stream, writer):
        ServesConnections.__init__(self)
        self._stream = stream
        self._writer = writer

    @classmethod
    def build(cls, stream, config):
        delayed_stream = DelayedReplayStream.build(stream, config)
        writer = ReplayStreamWriter.build(delayed_stream)
        return cls(delayed_stream, writer)

    async def _handle_connection(self, connection):
        await self._writer.send_to(connection)

    async def _after_connections_end(self):
        await self._stream.wait_for_ended()

    def __str__(self):
        return "Sender"
