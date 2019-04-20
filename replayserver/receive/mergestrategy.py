import asyncio
from enum import Enum
from replayserver.logging import logger
from replayserver.receive.offlinemerger import memprefix


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
            data = await stream.wait_for_data(position)
            position += len(data)
            if data != b"":
                self.new_data(stream)
            else:
                break
        self.stream_removed(stream)


class DivergenceTracking:
    """
    Allows us to compare stream with sink for divergence. Ensures that we never
    compare the same data twice.
    """
    def __init__(self, stream, sink):
        self._stream = stream
        self._sink = sink
        self.diverges = False
        self._compared_num = 0

    def check_divergence(self):
        if self.diverges:
            return
        start = self._compared_num
        end = min(len(self._stream.future_data), len(self._sink.data))
        if start >= end:
            return

        view1 = self._stream.data.view()
        view2 = self._sink.data.view()
        self.diverges = view1[start:end] != view2[start:end]
        self._compared_num = end
        view1.release()
        view2.release()


class QuorumState(Enum):
    QUORUM = 1
    STALEMATE = 2   # Includes having no streams at all


class QuorumRole(Enum):
    DIVERGED = 1
    CANDIDATE = 2
    QUORUM = 3
    STALEMATE_CANDIDATE = 4


class QuorumSets:
    def __init__(self, sink):
        self._sink = sink
        self._s2q = {}
        self.diverged = set()
        self.candidates = set()
        self.quorum = set()
        self.stalemate_candidates = {}

    def add_stream(self, stream):
        qs = QuorumStream(stream, self._sink)
        self._s2q[stream] = qs
        self.candidates.add(qs)

    def remove_stream(self, stream):
        qs = self._s2q[stream]
        self.make_qs_free(qs)
        del self._s2q[stream]

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
    def __init__(self, stream, sink):
        self.stream = stream
        self._div = DivergenceTracking(stream, sink)
        self.role = QuorumRole.CANDIDATE
        self.stalemate_byte = None
        self.ended = False

    @property
    def diverges(self):
        return self._div.diverges

    def check_divergence(self):
        self._div.check_divergence()


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
      data, we do that until we reach the quorum point again and have to
      recalculate it. Otherwise we enter a STALEMATE.
    - In a STALEMATE, we divide the quorum based on their next byte, discarding
      those that don't have a next byte. Then we look for tie breakers among
      other streams we have until some stream group has is large enough to be
      the new quorum. If some streams didn't reach the quorum point yet, we
      wait for them. If there are no more streams, we give up and pick
      whichever group is best - unless there are no groups at all, in which
      case we just wait.

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
      - All streams in quorum agree up to quorum point.
      - Quorum point is no smaller than sink stream length.
    - For both between calls, we observe:
      - Each diverged stream either has no more data than sink stream and
        ended, or does not agree with it.

    - The strategy can transition between states only on boundaries. These
      boundaries are, respectively:
      - For QUORUM, quorum point being equal to sink stream length,
      - For STALEMATE, existence of eligible candidate, that is:
        - At least one stalemate candidate set exists,
        - Either there are no candidates or at least one stalemate candidat has
          size of a desired quorum.
      Any time we reach a state boundary, we have work to do - either calculate
      the new quorum point, or resolve the stalemate. In particular, we are
      never on a state boundary between calls. We will never loop, either, see
      comments for state changing function below.
    """
    def __init__(self, sink):
        self.sink_stream = sink
        self.sets = QuorumSets()
        self._state = QuorumState.STALEMATE
        self._quorum_point = 0
        self._desired_quorum = 2

    @classmethod
    def build(cls, sink, config):
        return cls(sink)

    def _should_find_new_quorum_point(self):
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
        if max(len(i) for i in cands.values()) >= self.desired_quorum:
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
          new one (results in keeping the quorum or starting a stalemate),
        - In STALEMATE state when we can resolve the stalemate.

        Note that:
        - If we should find a new quorum point, then either we get at least one
          extra byte, or we transition into STALEMATE.
        - If we can resolve the stalemate, then we will get at least one extra
          byte and transition into QUORUM.
        Therefore, as long as we do work, we will always find extra data to
        send, so at some point we will stop working.
        """
        if self._should_find_new_quorum_point():
            self._find_new_quorum_point()
            return True
        elif self._can_resolve_stalemate():
            self._resolve_stalemate()
            return True
        return False

    def _resolve_stalemate(self):
        quorum_byte = self._get_best_stalemate_candidates()
        cands = self.sets.stalemate_candidates
        quorum = set(cands[quorum_byte])
        rest = {s for alt in cands.values() for s in alt}
        for qs in rest:
            self.sets.make_qs_diverged(qs)
        for qs in quorum:
            self.sets.make_qs_quorum(qs)
        self._state = QuorumState.QUORUM
        # Make sure we work on the new quorum
        assert len(self.sink_stream.data) == self._quorum_point

    def _get_best_stalemate_candidates(self):
        cands = self.sets.stalemate_candidates
        for cand in cands.values():
            if len(cand) >= self._desired_quorum:
                return cand

        def cand_rank(byte):
            cand = cands[byte]
            return (len(cand), max(len(qs.stream.future_data) for qs in cand))

        return max(cands, key=cand_rank)

    def _find_new_quorum_point(self):
        assert len(self.sink_stream.data) == self._quorum_point

        for qs in set(self.sets.quorum):
            if len(qs.stream.future_data) <= len(self.sink_stream.data):
                self.sets.make_qs_candidate(qs)

        self._fill_up_quorum()
        if (len(self.sets.quorum) < self._desired_quorum
                and self.sets.candidates):
            self._begin_stalemate()
            return

        new_quorum_point = self._get_new_quorum_point()
        if new_quorum_point <= len(self.sink_stream.data):
            self._begin_stalemate()
            return
        self._quorum_point = new_quorum_point
        self._send_new_quorum_data()

    def _get_new_quorum_point(self):
        # TODO
        return 0

    def _fill_up_quorum(self):
        for qs in set(self.sets.candidates):
            self._vet_for_quorum(qs)
            if len(self.sets.quorum) >= self._desired_quorum:
                break

    def _begin_stalemate(self):
        for qs in set(self.sets.quorum):
            data = qs.stream.future_data
            assert len(data) > self._quorum_point
            self.sets.make_qs_stalemate_candidate(qs, data[self._quorum_point])
        self._state = QuorumRole.STALEMATE

        for qs in set(self.sets.candidates):
            if self._can_resolve_stalemate():
                break
            self._vet_for_stalemate(qs)

    def _send_new_quorum_data(self):
        assert self._quorum_point > len(self.sink_stream.data)
        quorum = self.sets.quorum
        best_qs = quorum.max(key=lambda x: len(x.stream.data))
        self._add_quorum_data(best_qs)

    def _vet_for_quorum(self, qs):
        assert self._state is QuorumState.QUORUM
        assert qs.role is QuorumRole.CANDIDATE
        self._check_if_is_no_longer_candidate(qs)
        if self._stream_diverged(qs):
            return
        self.sets.make_qs_quorum(qs)

    def _vet_for_stalemate(self, qs):
        assert self._state is QuorumState.STALEMATE
        assert qs.role is QuorumRole.CANDIDATE
        self._check_if_is_no_longer_candidate(qs)
        if self._stream_diverged(qs):
            return
        stalemate_byte = qs.stream.future_data[len(self.sink_stream.data)]
        self.sets.make_qs_stalemate_candidate(qs, stalemate_byte)

    def _check_if_is_no_longer_candidate(self, qs):
        assert qs.role is QuorumRole.CANDIDATE
        # Should only be needed at stalemate or when finding new quorum point
        assert len(self.sink_stream.data) == self._quorum_point

        if len(qs.stream.future_data) <= len(self.sink_stream.data):
            if qs.ended:
                self.sets.make_qs_diverged(qs)
            return
        qs.check_divergence()
        if qs.diverges:
            self.sets.make_qs_diverged(qs)

    def _stream_diverged(self, qs):
        return qs in self.sets.diverged

    def stream_added(self, stream):
        self.sets.add_stream(stream)

    def stream_removed(self, stream):
        qs = self.sets.get_qs(stream)
        qs.ended = True
        if qs.role is not QuorumRole.CANDIDATE:
            return
        if self._state == QuorumState.STALEMATE:
            self._vet_for_stalemate(qs)

    def new_header(self, stream):
        if self.sink_stream.header is None:
            self.sink_stream.set_header(stream.header)

    def new_data(self, stream):
        if self._state == QuorumState.QUORUM:
            if stream not in self.sets.quorum:
                return
            self._add_quorum_data(self.sets.get_qs(stream))
            self._check_for_work()
        elif self._state == QuorumState.STALEMATE:
            self._vet_for_stalemate(self.sets.get_qs(stream))
            self._check_for_work()

    def _add_quorum_data(self, qs):
        send_from = len(self.sink_stream.data)
        send_to = min(len(qs.stream.data), self._desired_quorum)
        self.sink_stream.feed_data(qs.stream.data[send_from:send_to])

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


class FollowStreamMergeStrategy(MergeStrategy):
    """
    Tries its best to follow a single stream. If it ends, looks for a new
    stream to track, ensuring first that it has matching data.
    This strategy guarantees that the sink will equal a stream which is not a
    prefix of any other (a "maximal" stream). It does NOT guarantee that a most
    common stream will be picked, so if a replay we track diverges and
    terminates early, tough luck. It does, however, protect against a stream
    that stalls by periodically checking if any new data has been sent and
    switching to another stream if it hasn't.
    This is roughly the strategy of the original replay server.

    Invariants:
    0. Streams match iif one's data is a prefix of the other's.
    1. Tracked stream always matches and has at least as much data as sink.
    2. We don't track a stream iif no stream fits above condition.
    3. Matching set MUST contain all matching streams.
    4. Matching set MAY contain diverging streams. They are lazily removed
       when we check if a stream is fit to be tracked.
    5. After finalize(), ALL streams either diverge or are prefices of sink.
    """
    def __init__(self, sink_stream, config):
        MergeStrategy.__init__(self, sink_stream)
        self._candidates = {}
        self._tracked_value = None
        self._stalling_watchdog = asyncio.ensure_future(
            self._guard_against_stalling(config.stall_check_period))

    @classmethod
    def build(cls, sink_stream, config):
        return cls(sink_stream, config)

    @property
    def _tracked(self):
        return self._tracked_value

    @_tracked.setter
    def _tracked(self, val):
        if self._tracked_value is not None:
            logger.debug(f"Stopped tracking {self._tracked}")
        self._tracked_value = val
        if self._tracked_value is not None:
            logger.debug(f"Started tracking {self._tracked}")

    def _is_ahead_of_sink(self, stream):
        return len(stream.data) > len(self.sink_stream.data)

    def _eligible_for_tracking(self, stream):
        if not self._is_ahead_of_sink(stream):
            return False
        self._check_for_divergence(stream)
        return stream in self._candidates

    def _check_for_divergence(self, stream):
        check = self._candidates[stream]
        check.check_divergence()
        if check.diverges:
            logger.debug(f"{stream} diverges from canonical stream, removing")
            del self._candidates[stream]

    def _stream_has_diverged(self, stream):
        return stream not in self._candidates

    def _feed_sink(self):
        if self._tracked is None:
            return
        sink_len = len(self.sink_stream.data)
        self.sink_stream.feed_data(self._tracked.data[sink_len:])

    def _find_new_stream(self):
        for stream in list(self._candidates.keys()):
            if self._eligible_for_tracking(stream):
                self._tracked = stream
                break
        self._feed_sink()

    def stream_added(self, stream):
        self._candidates[stream] = DivergenceTracking(stream, self.sink_stream)

    def stream_removed(self, stream):
        if self._stream_has_diverged(stream):
            return
        # Don't remove a not-tracked stream - it might have more data that
        # matches currently tracked stream, in case it ends short!
        if stream is self._tracked:
            self._tracked = None
            self._candidates.pop(stream, None)
            self._find_new_stream()

    def new_data(self, stream):
        if self._stream_has_diverged(stream):
            return
        if self._tracked is None and self._eligible_for_tracking(stream):
            self._tracked = stream
        if stream is self._tracked:
            self._feed_sink()

    def finalize(self):
        self._stalling_watchdog.cancel()
        # Check any ended streams we saved for later
        while self._tracked is not None:
            self.stream_removed(self._tracked)
        self._candidates.clear()
        self.sink_stream.finish()

    def new_header(self, stream):
        if self.sink_stream.header is None:
            self.sink_stream.set_header(stream.header)

    async def _guard_against_stalling(self, stall_check_period):
        """
        Stops tracking a stream if it didn't advance for stall_check_period
        seconds, possibly finding a better one.
        This will always let us advance further if possible - the stream we
        just removed won't get picked again, since a stream needs to be
        strictly ahead of the sink to be eligible (and a tracked stream is
        always equal with it). Either we'll immediately pick a stream that's
        further ahead or won't track until first eligible stream appears.
        """
        current_pos = len(self.sink_stream.data)
        previous_pos = current_pos
        while True:
            previous_pos = current_pos
            await asyncio.sleep(stall_check_period)
            current_pos = len(self.sink_stream.data)
            if current_pos == previous_pos and self._tracked is not None:
                logger.debug((f"{self._tracked} has been stalling for "
                              f"{stall_check_period}s - stopping tracking"))
                self._tracked = None
                self._find_new_stream()
