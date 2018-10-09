from os import path
import json
import copy


__all__ = ["load_replay", "example_replay"]
replay_directory = path.join(path.dirname(path.realpath(__file__)), "data")


class RawReplay:
    def __init__(self, data, header_data, header_size):
        self.data = bytearray(data)
        self.header_data = header_data
        self.header_size = header_size

    def copy(self):
        return RawReplay(copy.copy(self.data), self.header_data,
                         self.header_size)


def load_replay(name, header_size):
    data = open(path.join(replay_directory, name), "rb").read()
    header_data = json.loads(
        open(path.join(replay_directory, f"{name}.header"), "rb").read())
    return RawReplay(data, header_data, header_size)


example_replay = load_replay("example", 1966)
