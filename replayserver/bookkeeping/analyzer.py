from fafreplay import Parser, ReplayReadError, commands
from replayserver.errors import BookkeepingError


class ReplayAnalyzer:
    def __init__(self):
        self._parser = Parser(
            commands=[commands.Advance],
            save_commands=False
        )

    def get_replay_ticks(self, data):
        """Parse the replay data and extract the total number of ticks

        Parameters
        ----------
        data: The replay body only (without the header). The first byte must be
            the start of a valid replay command.
        """
        if not data:
            raise BookkeepingError("No replay data")

        try:
            replay_body = self._parser.parse_body(bytes(data))

            return replay_body["sim"]["tick"]
        except ReplayReadError as e:
            raise BookkeepingError(f"Failed to parse replay: {e}")
