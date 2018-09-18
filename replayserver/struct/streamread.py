class GeneratorData:
    """
    A tiny ByteArray wrapper with support for pausing while reading data using
    generator-based coroutines.
    """
    def __init__(self, maxlen=None):
        self.data = bytearray()
        self.position = 0
        self._maxlen = maxlen

    def more(self):
        if self._maxlen is not None and self._maxlen <= len(self.data):
            raise ValueError
        more_data = yield
        self.data += more_data
        return len(more_data)

    def take(self, amount):
        next_pos = self.position + amount
        if self._maxlen is not None and next_pos > self._maxlen:
            raise ValueError
        data = self.data[self.position:next_pos]
        self.position += amount
        return data


class GeneratorWrapper:
    """
    Tiny wrapper for using generator like an object.
    """
    def __init__(self, gen):
        self._gen = gen
        self._gen.send(None)
        self._result = None

    def done(self):
        return self._result is not None

    def send(self, data):
        try:
            self._gen.send(data)
        except StopIteration as v:
            self._result = v.value

    def result(self):
        return self._result


def read_exactly(gen, amount):
    new_position = gen.position + amount
    while len(gen.data) < new_position:
        yield from gen.more()
    return gen.take(amount)


def read_until(gen, b):
    search_pos = gen.position

    def bytepos(b):
        return gen.data.find(b, search_pos)

    while bytepos(b) == -1:
        search_pos = len(gen.data)
        yield from gen.more()
    return gen.take(bytepos(b) - gen.position + 1)  # Take inclusive
