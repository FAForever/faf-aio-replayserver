from asyncio.locks import Event
import prometheus_client

from replayserver.server.connectionproducer import ConnectionProducer
from replayserver.bookkeeping.database import Database, DatabaseConfig
from replayserver.server.connections import Connections
from replayserver.server.replays import Replays
from replayserver.server.replay import ReplayConfig
from replayserver.bookkeeping.bookkeeper import Bookkeeper, BookkeeperConfig
from replayserver import config


class ServerConfig(config.Config):
    _options = {
        "port": {
            "parser": config.positive_int,
            "doc": "Replayserver port."
        },
        "prometheus_port": {
            "parser": config.positive_int,
            "doc": "Replayserver prometheus endpoint."
        },
        "connection_header_read_timeout": {
            "parser": config.positive_int,
            "doc": ("Time in seconds until we drop a connection that doesn't "
                    "send us the initial header. This is significant since FA "
                    "connects to the replayserver at lobby creation, but "
                    "starts sending data only once the lobby launches. It's "
                    "a good idea to set this to a few hours. Note that after "
                    "we read the header, a connection's lifetime is bounded "
                    "by lifetime of its replay, so no further configuration "
                    "is needed.")
        },
    }


class MainConfig(config.Config):
    _options = {
        "log_level": {
            "parser": int,
            "doc": ("Server log level. Numeric value corresponding to "
                    "Python's logging module value.")
        }
    }

    def __init__(self, config):
        super().__init__(config)
        self.server = ServerConfig(config.with_namespace("server"))
        self.db = DatabaseConfig(config.with_namespace("db"))
        self.storage = BookkeeperConfig(config.with_namespace("storage"))
        self.replay = ReplayConfig(config.with_namespace("config"))


class Server:
    def __init__(self, connection_producer, database,
                 connections, replays, bookkeeper,
                 prometheus_port):
        self._connection_producer = connection_producer
        self._database = database
        self._connections = connections
        self._replays = replays
        self._bookkeper = bookkeeper
        self._prometheus_port = prometheus_port
        self._stopped = Event()
        self._stopped.set()

    @classmethod
    def build(cls, *,
              dep_connection_producer=ConnectionProducer.build,
              dep_database=Database.build,
              config):
        database = dep_database(config.db)
        bookkeeper = Bookkeeper.build(database, config.storage)
        replays = Replays.build(bookkeeper, config.replay)
        conns = Connections.build(replays,
                                  config.server.connection_header_read_timeout)
        producer = dep_connection_producer(conns.handle_connection,
                                           config.server.port)
        return cls(producer, database, conns, replays, bookkeeper,
                   config.server.prometheus_port)

    async def start(self):
        if self._prometheus_port is not None:
            prometheus_client.start_http_server(self._prometheus_port)
        await self._database.start()
        await self._connection_producer.start()
        self._stopped.clear()

    async def stop(self):
        await self._connection_producer.stop()
        await self._connections.close_all()
        await self._replays.stop_all()
        await self._connections.wait_until_empty()
        await self._database.stop()
        self._stopped.set()

    async def run(self):
        await self.start()
        await self._stopped.wait()
