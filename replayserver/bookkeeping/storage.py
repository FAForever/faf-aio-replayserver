import os
import json
import base64
import struct
import zlib

from replayserver.errors import BookkeepingError


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


class ReplaySaver:
    def __init__(self, storage, database):
        self._storage = storage
        self._database = database

    async def save_replay(self, game_id, stream):
        if stream.header is None:
            raise BookkeepingError("Saved replay has no header!")
        info = await self._get_replay_info(game_id, stream.header.header)
        rfile = self._storage.get_replay_file(game_id)
        try:
            self._write_replay(rfile, info, stream.data.bytes())
        finally:
            rfile.close()

    async def _get_replay_info(self, game_id, header):
        result = {}
        result['uid'] = game_id
        result['complete'] = True
        result['state'] = 'PLAYING'

        try:
            result['sim_mods'] = {mod['uid']: mod['version']
                                  for mod in header.get('mods', []).values()}
        except KeyError:    # TODO - validate elsewhere?
            raise BookkeepingError("Replay header has invalid sim_mods")

        game_stats = await self._database.get_game_stats(game_id)
        teams = await self._database.get_teams_in_game(game_id)
        result.update(game_stats)
        result['teams'] = teams

        game_mod = game_stats[0]["game_mod"]
        featured_mods = await self._database.get_mod_versions(game_mod)
        result['featured_mod_versions'] = featured_mods
        return result

    async def _write_replay(self, rfile, info, data):
        try:
            rfile.write(json.dumps(info).encode('UTF-8'))
            rfile.write("\n")
            data = zlib.compress(struct.pack("i", len(data)) + data)
            data = base64.b64encode(data)
            rfile.write(data)
        except (UnicodeEncodeError, OSError):
            raise BookkeepingError("Failed to write out replay info")
