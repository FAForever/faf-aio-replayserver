from replay_server.connection import ReplayConnection


class ReplayWorkerBase:
    """
    Base for stream handlers
    """
    def __init__(self, connection: ReplayConnection, *args, **kwargs) -> None:
        self._connection: ReplayConnection = connection
        self.position: int = 0

    async def run(self):
        await self.process()
        await self._connection.close()
        await self.cleanup()

    async def process(self):
        raise NotImplemented("=^_^=")

    async def cleanup(self):
        raise NotImplemented("=^_^=")

    def get_uid(self):
        return self._connection.uid
