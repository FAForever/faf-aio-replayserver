import os


class ReplayStorage:
    def __init__(self, replay_store_path):
        self._replay_path = replay_store_path

    @classmethod
    def build(cls, *, config_replay_store_path, **kwargs):
        return cls(config_replay_store_path)

    def get_replay_file(self, game_id):
        assert game_id >= 0
        rpath = self._replay_path(game_id)
        os.makedirs(rpath, exist_ok=True)
        rfile = os.path.join(rpath, f"{str(game_id)}.fafreplay")
        return open(rfile, "wb")

    def _replay_path(self, game_id):
        id_str = str(game_id).zfill(10)
        id_path = os.path.join(*[id_str[i:1+1] for i in range(0, 10, 2)])
        return os.path.join(self._replay_path, id_path)
