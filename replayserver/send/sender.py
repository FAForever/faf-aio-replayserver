import asyncio
from asyncio.locks import Event
from contextlib import contextmanager

from replayserver.common import CanStopServingConnsMixin
from replayserver.send.stream import DelayedReplayStream
from replayserver.errors import MalformedDataError, \
    CannotAcceptConnectionError
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
                    "of isteners' sockets, as the server sets high/low buffer "
                    "water marks to 0 in order to prevent unwanted latency. "
                    "Setting this value higher might improve performance.")
        }
    }


class Sender(CanStopServingConnsMixin):
    def __init__(self, delayed_stream):
        CanStopServingConnsMixin.__init__(self)
        self._stream = delayed_stream
        self._ended = Event()
        asyncio.ensure_future(self._lifetime())

    @classmethod
    def build(cls, stream, config):
        delayed_stream = DelayedReplayStream.build(stream, config)
        return cls(delayed_stream)

    @contextmanager
    def _track_connection(self):
        self._connection_count.inc()
        try:
            yield
        finally:
            self._connection_count.dec()

    async def handle_connection(self, connection):
        if not self._accepts_connections():
            raise CannotAcceptConnectionError(
                "Reader connection arrived after replay ended")
        with self._track_connection():
            await self._write_header(connection)
            await self._write_replay(connection)

    async def _write_header(self, connection):
        header = await self._stream.wait_for_header()
        if header is None:
            raise MalformedDataError("Malformed replay header")
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

    async def _lifetime(self):
        await self._wait_until_all_connections_end()
        self._ended.set()

    async def wait_for_ended(self):
        await self._ended.wait()
