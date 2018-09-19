import zlib
import base64
import sys

if __name__ == "__main__":
    replay = open(sys.argv[1], "br").read()
    b64_part = replay.split(b'\n', 2)[1]
    zipped_part = base64.b64decode(b64_part)[4:]  # First 4 bytes are data size
    raw_replay_data = zlib.decompress(zipped_part)
    open(sys.argv[2], "bw").write(raw_replay_data)
