import asyncio
from asyncio import IncompleteReadError, LimitOverrunError
from builtins import ValueError
from typing import Optional, Tuple

from replay_server.constants import TERMINATOR, ACTION_TYPES, NEW_LINE
from replay_server.logger import logger

__all__ = ('ReplayConnection',)


class ReplayConnection:
    """
    Helper that determines actions on stream.
    """
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader: asyncio.StreamReader = reader
        self.writer: asyncio.StreamWriter = writer
        self.uid: int = None
        self.game_name: str = None

    async def get_type(self) -> Optional[int]:
        """
        Checks if received request has proper action. If not, None returned.
        """
        action = await self.reader.read(1)
        if action in ACTION_TYPES:
            if await self.reader.read(1) == NEW_LINE:
                return ACTION_TYPES.get(action)

        return None

    async def determine_type(self) -> Optional[int]:
        """
        Determines which action client is asking for.
        """
        logger.debug("<%s> Determining action type", self)
        try:
            action = await self.get_type()
            if action is None:
                raise ConnectionError("Determine type: Unexpected data received")
            return action
        except IncompleteReadError:
            raise ConnectionError("Disconnected before type was determined")

    async def get_replay_name(self) -> Tuple[int, str]:
        """
        Parses request and returns uid & name
        """
        logger.debug("<%s> Getting replay name", self)
        try:
            line = (await self.reader.readuntil(TERMINATOR))[:-1].decode()
            self.uid, self.game_name = line.split("/", 1)
            try:
                self.uid = int(self.uid)
            except ValueError:
                raise ConnectionError("Invalid data")
            return self.uid, self.game_name
        except IncompleteReadError:
            raise ConnectionError("Disconnected before receiving replay data")
        except (LimitOverrunError, ValueError, UnicodeDecodeError):
            raise ConnectionError("Replay name: Unexpected data received")

    async def close(self):
        logger.debug("<%s> Closing connection...", self)
        self.writer.close()
        logger.debug("<%s> Connection closed", self)

    def __str__(self):
        return str(id(self))
