def memprefix(b1, b2, start=0):
    """
    Returns length of longest common prefix of b1 and b2.
    There's no standard call for that and we don't want to compare byte-by-byte
    in the interpreter :/
    """
    end = min(len(b1), len(b2))
    chunk = 32 * 1024

    if start == end:
        return start

    while end - start > 16:
        while end - start > chunk:
            chunk //= 8
        if b1[start:start + chunk] == b2[start:start + chunk]:
            start += chunk
        else:
            end = start + chunk

    for c1, c2 in zip(b1[start:end], b2[start:end]):
        if c1 == c2:
            start += 1

    return start


class MergeInfo:
    def __init__(self, data, idx):
        self.data = data
        self.view = memoryview(data)    # Release that later!
        self.idx = idx
        self.confirmations = 0


class DataMerger:
    """
    Given a bunch of byte arrays, picks the "best" one. For two arrays A and B
    we say that A confirms B if A is a prefix of B. We choose to save an array
    with the highest number of other arrays confirming it.

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

    def add_replay(self, data):
        new = MergeInfo(data, len(self.replays))

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
            if common == len(old):
                new.confirmations += 1
            if common == len(new):
                old.confirmations += 1
        self.replays.append(new)

        # Move the replay with most confirmations to the front so we memcmp
        # with it first; most of the time it will save us the effort jumping
        # between replays
        mc = self.replays.index(
            max(self.replays, key=lambda x: x.confirmations))
        self.replays[0], self.replays[mc] = self.replays[mc], self.replays[0]

    def get_best_replay(self):
        best = max(self.replays, key=lambda x: x.confirmations)
        for data in self.replays:
            data.view.release()
        return best
