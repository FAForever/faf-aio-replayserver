[![Build Status](https://travis-ci.org/FAForever/faf-aio-replayserver.svg?branch=master)](https://travis-ci.org/FAForever/faf-aio-replayserver)
[![Coverage Status](https://coveralls.io/repos/github/FAForever/faf-aio-replayserver/badge.svg?branch=master)](https://coveralls.io/github/FAForever/faf-aio-replayserver?branch=master)

FAF replay server
=================

This is a prototype for the new FAF replay server. It uses asyncio a lot and
hopefully is better designed than the old replay server.

Documentation
-------------

You can find documentation in the doc directory. It's built with sphinx using
autodoc, so you'll need to install server dependencies to generate it properly -
see the 'dev environment' section. Docs can be built with:

`sphinx-build -b html ./doc ./build`

Documentation covers both server requirements and architecture.

Dev environment and testing
---------------------------

You'll need to setup faf-db for most work requiring a database. See
instructions at https://github.com/FAForever/db and .travis.yml for details on
how to start the container and populate it with test data. Once you setup
faf-db, run the setup\_db.py script to populate it with test data:

`python3 setup/db_setup.py`

For python packages, just install dependencies in requirements.txt and
requirements-test.txt in a virtualenv. Note that asynctest 0.12.2 throws errors
in a few tests - see issue #108 in asynctest's github for a fix you can apply
manually. Once everything is setup, you can run tests with:

`python3 -m pytest`

Server launching and configuration
----------------------------------

After installation, you should have `faf_replay_server` in your PATH. Running it
will launch the server. The server is configured via environment variables, as
follows:

- `MYSQL_{HOST,PORT,USER,PASSWORD,DB}` - respectively, host, port, user,
  password and database name to connect to. Naturally, should be an instance of
  faf-db.
- `PORT` - port to use for accepting connections.
- `REPLAY_DIR` - directory to save replays in.
- `PROMETHEUS_PORT` - port number for Prometheus endpoint.
- `LOG_LEVEL` - log level, as one of integer constants from Python's 'logging'
  module (e.g. 10 for DEBUG, 20 for INFO).
- `REPLAY_GRACE_PERIOD` - time in seconds Replay should wait after the last
  writer disconnects before assuming no more writers will connect and ending the
  replay.
- `REPLAY_FORCE_END_TIME` - time in seconds before a Replay is forcefully
  completed.
- `REPLAY_DELAY` - seconds of realtime delay between data received from readers
  and sent to writers. Used to prevent (trivial) cheating by playing the game
  and watching the replay at the same time.
- `SENT_REPLAY_UPDATE_INTERVAL` - replay stream timestamping interval, in
  seconds. Affects frequency of checking if there is new data to be sent to
  readers. Also affects frequency of calling `send()`, since asyncio write
  buffer is set to 0 - therefore increasing this might help performance.
- `CONNECTION_HEADER_READ_TIMEOUT` - time in seconds, after which a connection
  that did not send an initial header will be dropped. Suggested to be large,
  since connection starts at lobby launch and sends header at game launch.
- `REPLAY_MERGE_STRATEGY` - strategy to use for merging writer streams. One of
  MergeStrategies enum values.
- `MERGESTRATEGY_STALL_CHECK_PERIOD` - used by the 'follow stream' strategy.
  Interval, in seconds, after which a followed stream that did not send any data
  is swapped out for another.
