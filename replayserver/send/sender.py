from replayserver.common import ServesConnections
from replayserver.send.sendstrategy import SendStrategy
from replayserver.errors import CannotAcceptConnectionError
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
    def __init__(self, send_strategy):
        ServesConnections.__init__(self)
        self._strategy = send_strategy

    @classmethod
    def build(cls, stream, config):
        strategy = SendStrategy.build(stream, config)
        return cls(strategy)

    async def handle_connection(self, connection):
        if not self._accepts_connections():
            raise CannotAcceptConnectionError(
                "Reader connection arrived after replay ended")
        with self._count_connection():
            await self._strategy.send_to(connection)

    async def wait_for_ended(self):
        await self._wait_until_all_connections_end()
        await self._strategy.wait_for_stream()
