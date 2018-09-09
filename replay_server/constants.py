import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..'))

# logging
LOGGING_LEVEL = int(os.environ.get("LOGGING_LEVEL", 20))  # INFO = 20 DEBUG = 10 ERROR = 40

# server config
PORT = int(os.environ.get("PORT", 15000))
# Timeout step for waiting till next stream will come.
WAIT_STEP = 0.1  # in seconds, it might influence fluent streaming, default is 0.1
# Wait timeout, till we will send live stream back
REPLAY_TIMEOUT = int(os.environ.get("REPLAY_TIMEOUT", 60 * 5))
READ_BUFFER_SIZE = int(os.environ.get("READ_BUFFER_SIZE", 1024))
WRITE_BUFFER_SIZE = int(os.environ.get("WRITE_BUFFER_SIZE", 1024))
# base for uploading streams
TEMPORARY_DIR = os.environ.get("TEMPORARY_DIR", "./tmp/")
# base for uploaded streams
REPLAY_DIR = os.environ.get("REPLAY_DIR", "./replays/")

# Replay specific
TERMINATOR = b'\x00'  # null terminator

# actions for putting and getting stream
PUT_ACTION = b'P'
GET_ACTION = b'G'
NEW_LINE = b'/'


class RequestType:
    WRITER = 0  # saving stream
    READER = 1  # serving stream


ACTION_TYPES = {
    PUT_ACTION: RequestType.WRITER,
    GET_ACTION: RequestType.READER,
}


# Bug reporting configuration
BUG_SNAG_API_KEY = os.environ.get("BUG_SNAG_API_KEY", "3305a486a2f18fdcff9512c5a4e52406")

# mysql
MYSQL_DNS = {
    "host": os.environ.get("MYSQL_HOST", "127.0.0.1"),
    "port": int(os.environ.get("MYSQL_PORT", 3306)),
    "user": os.environ.get("MYSQL_USER", "root"),
    "password": os.environ.get("MYSQL_PASSWORD", "banana"),
    "db": os.environ.get("MYSQL_DB", "faf-dump"),
}

