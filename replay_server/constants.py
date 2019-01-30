import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..'))

# logging
LOGGING_LEVEL = int(os.environ.get("LOGGING_LEVEL", 20))  # INFO = 20 DEBUG = 10 ERROR = 40
ENV = os.environ.get("ENVIRONMENT", "development")  # in production it MUST be "production"
APP_VERSION = os.environ.get("APP_VERSION", "0.1")  # used for bugsnag logging

# server config
PORT = int(os.environ.get("PORT", 15000))
# Timeout step for waiting till next stream will come.
WAIT_STEP = float(os.environ.get("WAIT_STEP", 1))  # in seconds, it might influence fluent streaming, default is 0.1
READ_BUFFER_SIZE = 65535 + 1 + 2  # 65535 max tick size + 1 for type + 2 for tick content size
TICK_COUNT_TIMEOUT = int(os.environ.get("TICK_COUNT_TIMEOUT", 3000))  # 5 minutes * 10 tick per sec
WRITE_BUFFER_SIZE = int(os.environ.get("WRITE_BUFFER_SIZE", 1024))
DATABASE_WRITE_WAIT_TIME = float(os.environ.get("DATABASE_WRITE_WAIT_TIME", 10))
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
    "db": os.environ.get("MYSQL_DB", "faf"),
}
