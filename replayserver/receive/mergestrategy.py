from enum import Enum
from replayserver.receive.memprefix import memprefix


class MergeStrategy:
    def __init__(self, sink_stream):
        self.sink_stream = sink_stream

    def new_header(self, stream):
        raise NotImplementedError

    def new_data(self, stream):
        raise NotImplementedError

    def finalize(self):
        raise NotImplementedError

    # An added stream will always start with no header and no data.
    def stream_added(self, stream):
        raise NotImplementedError

    def stream_removed(self, stream):
        raise NotImplementedError

    # Convenience stuff.
    async def track_stream(self, stream):
        self.stream_added(stream)
        header = await stream.wait_for_header()
        if header is not None:
            self.new_header(stream)
        position = 0
        while True:
            dlen = await stream.wait_for_data(position)
            position += dlen
            if dlen != 0:
                self.new_data(stream)
            else:
                break
        self.stream_removed(stream)


class QuorumState(Enum):
    QUORUM = 1
    STALEMATE = 2   # Includes having no streams at all


class QuorumRole(Enum):
    DIVERGED = 1
    CANDIDATE = 2
    QUORUM = 3
    STALEMATE_CANDIDATE = 4


class QuorumSets:
    def __init__(self, sink, cmp_cutoff):
        self._sink = sink
        self._cmp_cutoff = cmp_cutoff
        self._s2q = {}
        self.diverged = set()
        self.candidates = set()
        self.quorum = set()
        self.stalemate_candidates = {}

    def add_stream(self, stream):
        qs = QuorumStream(stream, self._sink, self._cmp_cutoff)
        self._s2q[stream] = qs
        self.candidates.add(qs)

    def get_qs(self, stream):
        return self._s2q[stream]

    def make_qs_free(self, qs):
        sets = {
            QuorumRole.DIVERGED: self.diverged,
            QuorumRole.CANDIDATE: self.candidates,
            QuorumRole.QUORUM: self.quorum
        }
        if qs.role in sets:
            sets[qs.role].remove(qs)
        elif qs.role == QuorumRole.STALEMATE_CANDIDATE:
            sd = self.stalemate_candidates[qs.stalemate_byte]
            sd.remove(qs)
            if not sd:
                del self.stalemate_candidates[qs.stalemate_byte]
        qs.role = None

    def make_qs_diverged(self, qs):
        self.make_qs_free(qs)
        qs.role = QuorumRole.DIVERGED
        self.diverged.add(qs)

    def make_qs_candidate(self, qs):
        self.make_qs_free(qs)
        qs.role = QuorumRole.CANDIDATE
        self.candidates.add(qs)

    def make_qs_quorum(self, qs):
        self.make_qs_free(qs)
        qs.role = QuorumRole.QUORUM
        self.quorum.add(qs)

    def make_qs_stalemate_candidate(self, qs, byte):
        self.make_qs_free(qs)
        qs.role = QuorumRole.STALEMATE_CANDIDATE
        qs.stalemate_byte = byte
        self.stalemate_candidates.setdefault(byte, set()).add(qs)


class QuorumStream:
    def __init__(self, stream, sink, cmp_cutoff):
        self.stream = stream
        self._div = DivergenceTracking(stream, sink, cmp_cutoff)
        self.role = QuorumRole.CANDIDATE
        self.stalemate_byte = None
        self.ended = False

    @property
    def diverges(self):
        # If we were explicitly set as diverged, then don't check
        return self.role is QuorumRole.DIVERGED or self._div.diverges

    def check_divergence(self):
        self._div.check_divergence(self.ended)

    def set_as_matching(self, c):
        self._div.set_as_matching(c)


class DivergenceTracking:
    """
    Allows us to compare stream with sink for divergence. Ensures that we never
    compare the same data twice.
    """

    def __init__(self, stream, sink, cmp_cutoff):
        self._stream = stream
        self._sink = sink
        self.diverges = False
        self._compared_num = 0
        self._cmp_cutoff = cmp_cutoff

    def check_divergence(self, ended):
        if self.diverges:
            return

        stream_len = len(self._stream.future_data)
        sink_len = len(self._sink.data)

        if ended and stream_len <= sink_len:
            self.diverges = True
            return

        start = self._compared_num
        if self._cmp_cutoff is not None:
            max_cmp = stream_len - self._cmp_cutoff
            start = max(start, max_cmp)
        end = min(stream_len, sink_len)
        if start >= end:
            return

        view1 = self._stream.future_data.view(start, end)
        view2 = self._sink.future_data.view(start, end)
        self.diverges = view1 != view2
        self._compared_num = end
        view1.release()
        view2.release()

    def set_as_matching(self, c):
        self._compared_num = max(self._compared_num, c)


class QuorumMergeStrategy(MergeStrategy):
    """
    This strategy tries its best to keep a quorum of streams of a specified
    size. As long as all those streams agree about the data we should send, it
    sends the data. Once streams start disagreeing, it enters a stalemate and
    tries to find a new quorum of streams that agree on the next byte.

    In detail:
    - We start in a STALEMATE state, with no streams, quorum point set to 0,
      empty sink stream.
    - When in a QUORUM state, we calculate the furthest point at which all
      quorum streams agree, using their future data (e.g. 5 minutes into the
      future). We call this the quorum point. If that point lets us send more
      data, we do that until we reach the quorum point again. Once we reach it,
      we enter a STALEMATE.
    - In a STALEMATE, we divide the quorum based on their next byte, discarding
      those that don't have a next byte. Then we look for tie breakers among
      other streams we have until some stream group is large enough to be the
      new quorum. If some streams didn't reach the quorum point yet, we wait
      for them. If there are no more streams, we give up and pick whichever
      group is best - unless there are no groups at all, in which case we just
      wait.

    Invariants:
    - If we are in a STALEMATE state between calls, we observe:
      - Each stream is exactly one of diverged, candidate, stalemate candidate.
      - If a stream is a candidate, then it has no more data than quorum point
        and did not end.
      - All stalemate candidates agree up to quorum point and have at least one
        byte of data beyond it.
      - Quorum point equals sink stream length.
    - If we are in a QUORUM state between calls, we observe:
      - Each stream is exactly one of diverged, candidate, quorum.
      - Quorum is not empty.
      - All streams in quorum agree up to quorum point.
      - Quorum point is no smaller than sink stream length.
    - For both between calls, we observe:
      - Each diverged stream either has no more data than sink stream and
        ended, or does not agree with it.
      - A stream is not checked for divergence if it has less data than sink.

    - The strategy can transition between states only on boundaries. These
      boundaries are, respectively:
      - For QUORUM, quorum point being equal to sink stream length,
      - For STALEMATE, existence of eligible candidate, that is:
        - At least one stalemate candidate set exists,
        - Either there are no candidates or at least one stalemate candidate
          has size of a desired quorum.
      Any time we reach a state boundary, we have work to do - either enter or
      resolve the stalemate. In particular, we are never on a state boundary
      between calls. We will never loop, either, see comments for state
      changing function below.

    Stream data discarding rules:
    - The moment a stream is found to diverge, all its data is discarded and
      must never be accessed again.
    - We never need data that was verified to match sink.
    - For quorum streams, we never need data past current sink length.
    If comparison cutoff 'c' is set:
    - For a cutoff point 'p', data from non-quorum streams is discarded until
      p - c. We never need to access that data for future comparison.
    """

    def __init__(self, sink, desired_quorum, cmp_cutoff):
        MergeStrategy.__init__(self, sink)
        self.sets = QuorumSets(sink, cmp_cutoff)
        self._state = QuorumState.STALEMATE
        self._quorum_point = 0
        self._desired_quorum = desired_quorum
        self._cmp_cutoff = cmp_cutoff

    @classmethod
    def build(cls, sink, config):
        return cls(sink, config.desired_quorum,
                   config.stream_comparison_cutoff)

    def _quorum_point_reached(self):
        return (self._state == QuorumState.QUORUM and
                len(self.sink_stream.data) >= self._quorum_point)

    def _can_resolve_stalemate(self):
        if self._state != QuorumState.STALEMATE:
            return False
        # If we have no stalemate candidates, we CANNOT resolve!
        if not self.sets.stalemate_candidates:
            return False
        # If we ran out of candidates, we have to resolve now.
        if not self.sets.candidates:
            return True
        # Otherwise just check if we have a quorum.
        cands = self.sets.stalemate_candidates
        if max(len(i) for i in cands.values()) >= self._desired_quorum:
            return True
        return False

    def _check_for_work(self):
        should_check_again = True
        while should_check_again:
            should_check_again = self._change_state_once()

    def _change_state_once(self):
        """
        Check if we have work to do. That is:
        - In QUORUM state when we reached the quorum point and should find a
          new one (results in starting a stalemate, picking old quorum as
          candidates first),
        - In STALEMATE state when we can resolve the stalemate.

        Note that:
        - If we should find a new quorum point, we transition into STALEMATE.
        - If we can resolve the stalemate, then we will get at least one extra
          byte and transition into QUORUM.
        Therefore, as long as we do work, we will always find extra data to
        send, so at some point we will stop working.
        """
        if self._quorum_point_reached():
            self._begin_stalemate()
            return True
        elif self._can_resolve_stalemate():
            self._resolve_stalemate()
            return True
        return False

    def _resolve_stalemate(self):
        self._prepare_new_quorum()
        assert len(self.sets.quorum) <= self._desired_quorum
        self._state = QuorumState.QUORUM
        self._find_new_quorum_point()

    def _prepare_new_quorum(self):
        quorum_byte = self._get_best_stalemate_candidates()
        cands = self.sets.stalemate_candidates
        quorum = cands[quorum_byte].copy()
        rest = {s for b, alt in cands.items() for s in alt if b != quorum_byte}
        for qs in rest:
            self.sets.make_qs_diverged(qs)
        for qs in quorum:
            if len(self.sets.quorum) < self._desired_quorum:
                self.sets.make_qs_quorum(qs)
            else:
                self.sets.make_qs_candidate(qs)

    def _get_best_stalemate_candidates(self):
        cands = self.sets.stalemate_candidates

        def cand_rank(byte):
            cand = cands[byte]
            return (len(cand), max(len(qs.stream.future_data) for qs in cand))

        return max(cands, key=cand_rank)

    def _find_new_quorum_point(self):
        assert len(self.sink_stream.data) == self._quorum_point
        # Assert that quorum is correct.
        self._quorum_point = self._get_new_quorum_point()
        self._trim_streams_with_quorum()
        self._mark_quorum_as_matching()
        self._send_new_quorum_data()

    # Should only be called if we're guaranteed to get a better quorum point,
    # i.e. right after resolving a stalemate.
    def _get_new_quorum_point(self):
        shortest_quorum = min(self.sets.quorum,
                              key=lambda x: len(x.stream.future_data))
        old_point = self._quorum_point
        max_dist = len(shortest_quorum.stream.future_data)
        best_common = max_dist - old_point

        sq_view = shortest_quorum.stream.future_data.view(start=old_point)
        for qs in self.sets.quorum:
            if qs is shortest_quorum:
                continue
            qs_view = qs.stream.future_data.view(start=old_point)
            best_common = memprefix(sq_view, qs_view, best_common)
            qs_view.release()
        sq_view.release()

        assert best_common > 0
        return old_point + best_common

    def _begin_stalemate(self):
        old_quorum = self.sets.quorum.copy()
        for qs in old_quorum:
            self.sets.make_qs_candidate(qs)

        self._state = QuorumState.STALEMATE

        # Check old quorum first.
        for qs in old_quorum:
            self._vet_for_stalemate(qs)
        for qs in self.sets.candidates.copy():
            if self._can_resolve_stalemate():
                break
            self._vet_for_stalemate(qs)

    def _send_new_quorum_data(self):
        assert self._quorum_point > len(self.sink_stream.data)
        quorum = self.sets.quorum
        best_qs = max(quorum, key=lambda x: len(x.stream.data))
        self._add_quorum_data(best_qs)

    def _vet_for_stalemate(self, qs):
        assert self._state is QuorumState.STALEMATE
        assert qs.role is QuorumRole.CANDIDATE
        self._check_if_diverged(qs)
        if qs.diverges or not self._candidate_has_enough_data(qs):
            return
        stalemate_byte = qs.stream.future_data[len(self.sink_stream.data)]
        self.sets.make_qs_stalemate_candidate(qs, stalemate_byte)

    def _check_if_diverged(self, qs):
        assert qs.role is QuorumRole.CANDIDATE
        # Should only be needed at stalemate or when finding new quorum point
        assert len(self.sink_stream.data) == self._quorum_point

        if not self._candidate_has_enough_data(qs) and not qs.ended:
            return
        qs.check_divergence()
        if qs.diverges:
            self.sets.make_qs_diverged(qs)
            qs.stream.discard_all()
        else:
            qs.stream.discard(len(self.sink_stream.data))

    def _candidate_has_enough_data(self, qs):
        return len(qs.stream.future_data) > len(self.sink_stream.data)

    def _trim_streams_with_quorum(self):
        if self._cmp_cutoff is None:
            return
        discard_size = self._quorum_point - self._cmp_cutoff
        for qs in self.sets.candidates:
            qs.stream.discard(discard_size)

    def _mark_quorum_as_matching(self):
        for qs in self.sets.quorum:
            qs.set_as_matching(self._quorum_point)

    def stream_added(self, stream):
        self.sets.add_stream(stream)

    def stream_removed(self, stream):
        qs = self.sets.get_qs(stream)
        qs.ended = True
        if qs.role is not QuorumRole.CANDIDATE:
            return
        if self._state == QuorumState.STALEMATE:
            self._vet_for_stalemate(qs)
            self._check_for_work()

    def new_header(self, stream):
        if self.sink_stream.header is None:
            self.sink_stream.set_header(stream.header)

    def new_data(self, stream):
        qs = self.sets.get_qs(stream)
        if self._state is QuorumState.QUORUM:
            if qs.role is not QuorumRole.QUORUM:
                return
            self._add_quorum_data(qs)
            self._check_for_work()
        elif self._state is QuorumState.STALEMATE:
            if qs.role is not QuorumRole.CANDIDATE:
                return
            self._vet_for_stalemate(qs)
            self._check_for_work()

    def _add_quorum_data(self, qs):
        send_from = len(self.sink_stream.data)
        send_to = min(len(qs.stream.data), self._quorum_point)
        if send_from < send_to:
            self.sink_stream.feed_data(qs.stream.data[send_from:send_to])
        qs.stream.discard(len(self.sink_stream.data))

    def finalize(self):
        # All streams sent all their data, so we must have sent everything up
        # to the last quorum point.
        assert self._quorum_point == len(self.sink_stream.data)
        # We never end work at state boundaries, so we must be in a stalemate.
        assert self._state == QuorumState.STALEMATE
        # All streams have ended, and every candidate must not have.
        assert not self.sets.candidates
        # Therefore, because we are not on a state boundary, there are no
        # stalemate candidates.
        assert not self.sets.stalemate_candidates
        # Therefore, all streams are considered diverged.
        self.sink_stream.finish()
