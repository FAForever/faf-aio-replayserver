from os import path


__all__ = ["load_replay"]
replay_directory = path.join(path.dirname(path.realpath(__file__)), "data")


def load_replay(name):
    return open(path.join(replay_directory, name), "rb").read()
