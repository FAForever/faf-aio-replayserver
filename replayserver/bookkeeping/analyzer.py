from fafreplay import ReplayReadError, body_ticks
from replayserver.errors import BookkeepingError


class ReplayAnalyzer:
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
            return body_ticks(data)
        except ReplayReadError as e:
            raise BookkeepingError(f"Failed to parse replay: {e}")
