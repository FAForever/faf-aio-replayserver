from replayserver.errors import BookkeepingError


class Bookkeeper:
    def __init__(self):
        pass

    async def save_replay(self):
        # Todo - args and members
        try:
            self._storage.save_replay(game_id, stream)
            self._database.register_replay(game_id)
        except BookkeepingError:
            pass    # TODO - log
