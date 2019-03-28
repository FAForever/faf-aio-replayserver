from replayserver.common import ServesConnections
from replayserver.send.stream import DelayedReplayStream, ReplayStreamWriter
from replayserver import config


class SenderConfig(config.Config):
    _options = {
        "replay_delay": {
            "parser": config.nonnegative_float,
            "doc": ("Delay in seconds between receiving replay data and "
                    "sending it to readers. Used to prevent cheating via "
                    "playing and observing at the same time.")
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
        writer = ReplayStreamWriter(delayed_stream)
        return cls(delayed_stream, writer)

    async def _handle_connection(self, connection):
        await self._writer.send_to(connection)

    async def _after_connections_end(self):
        await self._stream.wait_for_ended()

    def __str__(self):
        return "Sender"
