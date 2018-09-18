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
        if self._maxlen < len(self.data):
            raise ValueError
        more_data = yield
        self.data += more_data
        return len(more_data)

    def take(self, amount):
        data = self.data[self.position:self.position+amount]
        self.position += amount
        return data


class GeneratorWrapper:
    """
    Tiny wrapper for using generator like an object.
    """
    def __init__(self, gen, ):
        self._data = GeneratorData()
        self._last_value = self._gen.send(None)
        self._result = None

    def done(self):
        return self._result is not None

    def feed(self, data):
        try:
            self._last_value = self._gen.send(data)
        except StopIteration:
            self._result = self._last_value

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
        search_pos += yield from gen.more()
    target_pos = search_pos + bytepos(b)
    return gen.take(target_pos - gen.position)
