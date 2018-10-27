import os
import json
import base64
import struct
import zlib
import asyncio

from replayserver.errors import BookkeepingError


class ReplayFilePaths:
    def __init__(self, replay_store_path):
        self._replay_base_path = replay_store_path

    @classmethod
    def build(cls, *, config_replay_store_path, **kwargs):
        return cls(config_replay_store_path)

    def get(self, game_id):
        rpath = self._replay_path(game_id)
        os.makedirs(rpath, exist_ok=True)
        rfile = os.path.join(rpath, f"{str(game_id)}.fafreplay")
        if os.path.exists(rfile):
            raise BookkeepingError(f"Replay file {rfile} already exists")
        open(rfile, 'a').close()    # Touch file
        return rfile

    def _replay_path(self, game_id):
        # Legacy folder structure:
        # digits 3-10 from the right,
        digits = str(game_id).zfill(10)[-10:-2]
        # in 4 groups by 2 starting by most significant,
        groups = [digits[i:i+2] for i in range(0, len(digits), 2)]
        # NOT left-padded, so 0x -> x
        dirs = [str(int(g)) for g in groups]
        id_path = os.path.join(*dirs)
        return os.path.join(self._replay_base_path, id_path)


class ReplaySaver:
    def __init__(self, paths, database):
        self._paths = paths
        self._database = database

    @classmethod
    def build(cls, database, **kwargs):
        paths = ReplayFilePaths.build(**kwargs)
        return cls(paths, database)

    async def save_replay(self, game_id, stream):
        if stream.header is None:
            raise BookkeepingError("Saved replay has no header")
        info = await self._get_replay_info(game_id, stream.header.struct)
        rfile = self._paths.get(game_id)
        try:
            with open(rfile, "wb") as f:
                await self._write_replay_in_thread(
                    f, info, stream.header.data + stream.data.bytes())
        except IOError as e:
            raise BookkeepingError("Could not write to replay file") from e

    async def _get_replay_info(self, game_id, header):
        result = {}
        result['uid'] = game_id
        result['complete'] = True
        result['state'] = 'PLAYING'
        try:
            result['sim_mods'] = {
                mod['uid']: mod['version']
                for mod in header.get('mods', {}).values()
            }
        except KeyError:    # TODO - validate elsewhere?
            raise BookkeepingError("Replay header has invalid sim_mods")

        game_stats = await self._database.get_game_stats(game_id)
        teams = await self._database.get_teams_in_game(game_id)
        result.update(game_stats)
        result['teams'] = self._fixup_team_dict(teams)

        game_mod = game_stats["featured_mod"]
        featured_mods = await self._database.get_mod_versions(game_mod)
        result['featured_mod_versions'] = featured_mods
        return result

    def _fixup_team_dict(self, d):
        # Replay format uses strings for teams for some reason
        return {str(t) if t is not None else "null": p for t, p in d.items()}

    async def _write_replay_in_thread(self, rfile, info, data):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            lambda: self._write_replay(rfile, info, data))

    def _write_replay(self, rfile, info, data):
        try:
            rfile.write(json.dumps(info).encode('UTF-8'))
            rfile.write(b"\n")
            data = struct.pack("i", len(data)) + zlib.compress(data)
            data = base64.b64encode(data)
            rfile.write(data)
        except UnicodeEncodeError:
            raise BookkeepingError("Unicode encoding error")
