from os import path
import copy


__all__ = ["load_replay", "example_replay"]
replay_directory = path.join(path.dirname(path.realpath(__file__)), "data")


class RawReplay:
    def __init__(self, data, header_size):
        self.data = bytearray(data)
        self.header_size = header_size

    def copy(self):
        return RawReplay(copy.copy(self.data), copy.copy(self.header_size))


def load_replay(name):
    return open(path.join(replay_directory, name), "rb").read()


example_replay = RawReplay(load_replay("example"), 1966)
