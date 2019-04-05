from replayserver.struct.mangling import StreamDemangler


def test_mangler_withholds_eor():
    d = StreamDemangler()
    data = d.demangle(b"aaa")
    data += d.demangle(b"bcd\x17")
    data += d.demangle(b"\x03\x00")
    data += d.drain()
    assert data == b"aaabcd"


def test_mangler_allows_safe_data():
    d = StreamDemangler()
    data = d.demangle(b"aaa\x17\x03")
    data += d.demangle(b"cdef")
    data += d.demangle(b"\x17\x03\x00")
    data += d.demangle(b"zzzzz")
    data += d.drain()
    assert data == b"aaa\x17\x03cdef\x17\x03\x00zzzzz"
