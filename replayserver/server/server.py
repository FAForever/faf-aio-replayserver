from asyncio.locks import Event

from replayserver.server.connectionproducer import ConnectionProducer
from replayserver.bookkeeping.database import Database
from replayserver.server.connections import Connections
from replayserver.server.replays import Replays
from replayserver.bookkeeping.bookkeeper import Bookkeeper


class Server:
    def __init__(self, connection_producer, database,
                 connections, replays, bookkeeper):
        self._connection_producer = connection_producer
        self._database = database
        self._connections = connections
        self._replays = replays
        self._bookkeper = bookkeeper
        self._stopped = Event()
        self._stopped.set()

    @classmethod
    def build(cls, *,
              dep_connection_producer=ConnectionProducer.build,
              dep_database=Database.build,
              **kwargs):
        database = dep_database(**kwargs)
        bookkeeper = Bookkeeper.build(database, **kwargs)
        replays = Replays.build(bookkeeper, **kwargs)
        conns = Connections.build(replays, **kwargs)
        producer = dep_connection_producer(conns.handle_connection, **kwargs)
        return cls(producer, database, conns, replays, bookkeeper)

    async def start(self):
        await self._database.start()
        await self._connection_producer.start()
        self._stopped.clear()

    async def stop(self):
        await self._connection_producer.stop()
        self._connections.close_all()
        await self._replays.stop_all()
        await self._connections.wait_until_empty()
        await self._database.stop()
        self._stopped.set()

    async def run(self):
        await self.start()
        await self._stopped.wait()
