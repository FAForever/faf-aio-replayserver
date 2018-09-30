import asyncio
import base64
import json
import struct
import time
import zlib
from typing import List, Dict

from replay_server.constants import DATABASE_WRITE_WAIT_TIME
from replay_server.db_conn import db
from replay_server.logger import logger
from replay_server.replay_parser.replay_parser.parser import parse
from replay_server.utils.greatest_common_replay import get_replay
from replay_server.utils.paths import get_replay_path


async def save_replay(uid: int, file_paths: List[str]) -> None:
    """
    Saves completed replay.
    """
    logger.info("Saving data for uid %s", uid)
    logger.debug("Paths %s", str(list(file_paths)))
    replay_path = get_replay(file_paths)
    output_path = get_replay_path(uid)

    with open(output_path, "wb") as output_file:
        with open(replay_path, "rb") as replay_file:
            replay_data = replay_file.read()
            output_file.write(json.dumps(await get_replay_info(uid, replay_data)).encode('raw_unicode_escape'))
            output_file.write(b'\n')
            output_file.write(base64.b64encode(zlib.compress(struct.pack("i", len(replay_data)) + replay_data, 9)))


async def get_replay_info(game_id: int, replay_data: bytes) -> Dict:
    """
    Returns "header" information for replay.
    """
    logger.info("Collecting replay info for uid %s", game_id)
    result = {'uid': game_id}
    try:
        header = parse(replay_data)['header']
        game_version = header.get("version")
        result['sim_mods'] = {mod['uid']: mod['version'] for mod in header.get('mods', []).values()}

        try:
            game_stats = await get_game_stats(game_id)
            # situation, when we don't wanna loose all information, if mysql is down
            if not game_stats:
                return result

            logger.info("Querying replay info for uid %s from datbase", game_id)
            players = await get_players(game_id)
            featured_mods = await get_mod_updates(game_stats[0].get("game_mod"), game_version)
            game_stats_first_row = game_stats[0]

            teams = {}
            for player in players:
                teams.setdefault(player['team'], []).append(player['login'])

            featured_mod_versions = {}
            for mod in featured_mods:
                featured_mod_versions[str(mod['file_id'])] = mod['version']

            return dict(**result, **{
                'featured_mod': game_stats_first_row['game_mod'],
                'num_players': len(game_stats),
                'game_type': int(game_stats_first_row['game_type']),
                'recorder': game_stats_first_row['host'],
                'host': game_stats_first_row['host'],
                'launched_at': time.mktime(game_stats_first_row['start_time'].timetuple()),
                'game_end': time.mktime(game_stats_first_row['end_time'].timetuple()),
                'complete': True,
                'state': 'PLAYING',
                'title': game_stats_first_row['game_name'],
                'mapname': game_stats_first_row['map_name'],
                'map_file_path': game_stats_first_row['file_name'],
                'teams': teams,
                'featured_mod_versions': featured_mod_versions,
            })
        except Exception:
            logger.exception("Exception occured during getting replay info %s", game_id)
            raise
    except Exception:
        logger.error("Exception during getting information about replay %s", game_id)
        raise


async def _get_game_stats(game_id: int):
    """
    Check, if result is saved already in database.
    """
    game_stats = None
    await asyncio.sleep(DATABASE_WRITE_WAIT_TIME)
    for _ in range(5):
        game_stats = await get_game_stats(game_id)
        # check if result is correct
        if bool(game_stats) and bool(game_stats[0]['start_time']) and bool(game_stats[0]['end_time']):
            return game_stats
        await asyncio.sleep(DATABASE_WRITE_WAIT_TIME)
    return game_stats


async def get_game_stats(game_id: int):
    """
    Gets the game information.
    """
    query = """
        SELECT
            `game_stats`.`startTime` AS start_time,
            `game_stats`.`endTime` AS end_time,
            `game_stats`.`gameType` AS game_type,
            `login`.`login` AS host,
            `game_stats`.`gameName` AS game_name,
            `game_featuredMods`.`gamemod` AS game_mod,
            `map`.`display_name` as map_name,
            `map_version`.`filename` AS file_name,
            `game_player_stats`.`playerId` AS player_id,
            `game_player_stats`.`AI` AS ai
        FROM `game_stats`
        INNER JOIN `game_player_stats`
          ON `game_player_stats`.`gameId` = `game_stats`.`id`
        INNER JOIN `map`
          ON `game_stats`.`mapId` = `map`.`id`
        INNER JOIN `map_version`
          ON `map_version`.`map_id` = `map`.`id`  
        INNER JOIN `login`
          ON `login`.id = `game_stats`.`host`
        INNER JOIN  `game_featuredMods` ON `game_stats`.`gameMod` = `game_featuredMods`.`id`
        WHERE `game_stats`.`id` = %s
    """
    return await db.execute(query, (game_id,))


async def get_players(game_id):
    """
    Returns players in game
    """
    query = """
        SELECT
            `login`.`login` AS login,
            `game_player_stats`.`team` AS team
        FROM `game_stats`
        INNER JOIN `game_player_stats`
          ON `game_player_stats`.`gameId` = `game_stats`.`id`
        INNER JOIN `login`
          ON `login`.id = `game_player_stats`.`playerId`
        WHERE `game_stats`.`id` = %s
    """
    return await db.execute(query, (game_id,))


async def get_mod_updates(mod: str, game_version: str):
    """
    Gets last file changes for game version.
    """
    query_params = []
    filter_ = ""
    version = game_version.rsplit(".", 1)
    if len(version) == 2:
        filter_ = "WHERE version <= %s"
        query_params.append(version[1])

    query = """
        SELECT
            `updates_{mod}_files`.`fileId` AS file_id,
            MAX(`updates_{mod}_files`.`version`) AS version
        FROM `updates_{mod}`
        LEFT JOIN `updates_{mod}_files` ON `fileId` = `updates_{mod}`.`id`
        {filter}
        GROUP BY `updates_{mod}_files`.`fileId`
    """.format(mod=mod, filter=filter_)
    return await db.execute(query, query_params)
