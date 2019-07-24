from os import path
import json
import base64
import zlib
import zstandard as zstd
import copy
import sys


__all__ = ["load_replay", "example_replay", "diverging_1"]
replay_directory = path.join(path.dirname(path.realpath(__file__)), "data")


class RawReplay:
    def __init__(self, data, header, header_size):
        self.data = bytearray(data)
        self.header = header
        self.header_size = header_size

    @property
    def header_data(self):
        return self.data[:self.header_size]

    @property
    def main_data(self):
        return self.data[self.header_size:]

    def copy(self):
        return RawReplay(copy.copy(self.data), self.header,
                         self.header_size)


def load_replay(name, header_size):
    data = open(path.join(replay_directory, name), "rb").read()
    header = json.loads(
        open(path.join(replay_directory, f"{name}.header"), "rb").read())
    return RawReplay(data, header, header_size)


# Old format, but with zstd compression and no base64.
def unpack_replay_format_2(replay):
    head, zipped_part = replay.split(b'\n', 1)
    head = json.loads(head)
    raw_replay_data = zstd.ZstdDecompressor().decompress(zipped_part)
    return head, raw_replay_data


example_replay = load_replay("example", 1966)
# Trie of replay
# len 0 to 0
#  -len 7025 to 7025
#  | -Ends: 0, len 135 to 7160
#  | -Ends: 1, len 18499 to 25524
#  | | -len 701 to 26225
#  | | | -Ends: 2, len 39 to 26264
#  | | | -Ends: 4, len 871 to 27096
#  | | | | -Ends: 3, len 11 to 27107
#  | | | | | -Ends: 5, len 14 to 27121
diverging_1 = [load_replay(f"diverging_1/{i}", 3574) for i in range(6)]


if __name__ == "__main__":
    data = open(sys.argv[1], "rb").read()
    h, r = unpack_replay_format_2(data)
    open(sys.argv[2], "wb").write(r)
