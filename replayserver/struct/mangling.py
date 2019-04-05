END_OF_REPLAY = b"\x17\x03\x00"


class StreamDemangler:
    """
    We want replay streams we receive to be such that one is often a prefix of
    another. Unfortunately all streams end with an "end of stream" 3-byte
    message, which screws it up. We fix that here.
    """

    def __init__(self):
        self._withheld = b""

    def demangle(self, data):
        self._withheld = self._withheld + data
        if END_OF_REPLAY[0] not in self._withheld[-3:]:
            # No risk of returning stream end marker
            data = self._withheld
            self._withheld = b""
            return data
        else:
            data = self._withheld[:-3]
            self._withheld = self._withheld[-3:]
            return data

    def drain(self):
        # If there was a reasonable thing to do with the last withheld bit,
        # we'd do it here. Thing is, we can't 100% tell if that last bit is
        # legit data, end of stream or part data and partial end-of-stream.
        # Just assume that 99% of streams end with correct EOS, and if they did
        # not, then we probably already lost more data than 3 measly bytes.
        return b""


class StreamMangler:
    def mangle(self, data):
        return data

    def drain(self):
        # As above - if the stream is legit, we're just adding back the EOR
        # message. Otherwise, we probably lost data anyway, so a bit of
        # corruption at the end won't hurt us more.
        return END_OF_REPLAY
