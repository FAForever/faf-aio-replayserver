from os import path
import json
import base64
import zlib
import copy


__all__ = ["load_replay", "example_replay"]
replay_directory = path.join(path.dirname(path.realpath(__file__)), "data")


class RawReplay:
    def __init__(self, data, header, header_size):
        self.data = bytearray(data)
        self.header = header
        self.header_size = header_size

    def copy(self):
        return RawReplay(copy.copy(self.data), self.header,
                         self.header_size)


def load_replay(name, header_size):
    data = open(path.join(replay_directory, name), "rb").read()
    header = json.loads(
        open(path.join(replay_directory, f"{name}.header"), "rb").read())
    return RawReplay(data, header, header_size)


def unpack_replay(replay):
    head, b64_part = replay.split(b'\n', 1)
    head = json.loads(head)
    zipped_part = base64.b64decode(b64_part)[4:]  # First 4 bytes are data size
    raw_replay_data = zlib.decompress(zipped_part)
    return head, raw_replay_data


example_replay = load_replay("example", 1966)
