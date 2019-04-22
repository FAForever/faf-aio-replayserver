__all__ = ["memprefix", "DataMerger", "OfflineReplayMerger"]


def memprefix(b1, b2, start=0, end=None):
    """
    Returns length of longest common prefix of b1 and b2.
    There's no standard call for that and we don't want to compare byte-by-byte
    in the interpreter :/
    """
    if end is None:
        end = min(len(b1), len(b2))
    else:
        end = min(len(b1), len(b2), end)
    assert start <= end
    if start == end:
        return start

    # Short-circuit if streams are different immediately.
    if b1[start] != b2[start]:
        return start

    chunk = 32 * 1024

    while end - start > 16:
        while chunk >= end - start:
            chunk //= 8
        if b1[start:start + chunk] == b2[start:start + chunk]:
            start += chunk
        else:
            end = start + chunk

    for c1, c2 in zip(b1[start:end], b2[start:end]):
        if c1 == c2:
            start += 1
        else:
            break

    return start


class MergeInfo:
    def __init__(self, data, tag, idx):
        self.data = data
        self.tag = tag
        self.view = memoryview(data)    # Release that later!
        self.idx = idx
        self.rank = 0


class DataMerger:
    """
    Given a bunch of byte arrays, picks the "best" one. FA replays that end
    short tend to differ on the last couple hundred bytes, so we need some
    simple metric as to what "best" means. The heuristic here is just to sum
    all common prefix lengths of a given stream and all others - it eliminates
    streams that diverged alone or ended early, and in all other cases any
    choice is good.

    For that purpose we build a matrix M such that M[i][j] holds the length of
    the longest common prefix of arrays i and j. It's pretty easy to add arrays
    to such a matrix while comparing only O(n) bytes.
    """

    def __init__(self):
        self.prefix = {}
        self.replays = []

    def set_common(self, d1, d2, common):
        self.prefix.setdefault(d1.idx, {})[d2.idx] = common
        self.prefix.setdefault(d2.idx, {})[d1.idx] = common

    def get_common(self, d1, d2):
        return self.prefix[d1.idx][d2.idx]

    def _best_data(self):
        return max(self.replays, key=lambda x: (x.rank, len(x.data)))

    def add_data(self, data, tag):
        new = MergeInfo(data, tag, len(self.replays))

        # Get the empty case out of the way
        if not self.replays:
            self.replays.append(new)
            return

        # best_match is always a replay matching the largest prefix so far
        best_match = self.replays[0]
        best_common = memprefix(new.view, best_match.view)
        self.set_common(new, best_match, best_common)

        for old in self.replays[1:]:
            old_common = self.get_common(old, best_match)
            if old_common != best_common:
                # We differ with the same replay in different places, so we
                # differ with each other at the minimum
                old_common = min(best_common, old_common)
            else:
                old_common = memprefix(old.view, new.view, start=old_common)
            self.set_common(new, old, old_common)
            if old_common > best_common:
                best_common = old_common
                best_match = old

        # Update confirmation numbers
        for old in self.replays:
            common = self.get_common(old, new)
            new.rank += common
            old.rank += common
        self.replays.append(new)

        # Move the replay with highest rank to the front so we memcmp with it
        # first; most of the time it will save us the effort jumping between
        # replays
        mc = self.replays.index(self._best_data())
        self.replays[0], self.replays[mc] = self.replays[mc], self.replays[0]

    def get_best_data(self):
        if not self.replays:
            return None
        best = self._best_data()
        for data in self.replays:
            data.view.release()
        return best.tag


class OfflineReplayMerger:
    def __init__(self, merger):
        self._merger = merger

    @classmethod
    def build(cls):
        return cls(DataMerger())

    def add_replay(self, replay):
        data = replay.data.bytes()
        if data == b"":
            return

        # NOTE: Dicts in FA replay headers are serialized in random order. That
        # makes trying to merge headers useless unless we parse them in
        # entirety, and that's not really worth the trouble. "Best replay" will
        # probably have the correct header 99,9% of the time anyway.
        self._merger.add_data(data, replay)

    def get_best_replay(self):
        return self._merger.get_best_data()
