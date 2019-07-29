__all__ = ["memprefix"]


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
