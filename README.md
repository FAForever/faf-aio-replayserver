[![Build Status](https://travis-ci.org/norraxx/faf-aio-replayserver.svg?branch=master)](https://travis-ci.org/norraxx/faf-aio-replayserver)
[![Coverage Status](https://coveralls.io/repos/github/norraxx/faf-aio-replayserver/badge.svg?branch=master)](https://coveralls.io/github/norraxx/faf-aio-replayserver?branch=master)

# Replay server

Read streams from users and stores them into the files on server side.
Also can read stream most common stream to other users.
After the game is end, chooses the biggest common streams and saves them.


## Configuring project for development:

    - git clone https://github.com/FAForever/faf-aio-replayserver.git
    - cd faf-aio-replayserver
    - git submodule init
    - git submodule update
    - cd ..
    - git clone https://github.com/FAForever/faf-stack.git
    - cd faf-stack
    - docker-compose up faf-db
    - configure connection to MySQL databse in faf-aio-replayserver/replay_server/constants.py


## Running project via faf-stack:

    - git clone https://github.com/FAForever/faf-stack.git
    - cd faf-stack
    - docker-compose up faf-aio-replayserver


## Project directory structure:

    - replay_server - contains code
        - __main__.py - starts server
        - replay_parser/ - SCFA replay parser, used for reading replay headers
        - stream/ - stream handlers for saving and serving streams
            - factory.py - factory for request handlers
            - reader.py - streams "live replay"
            - writer.py - write replay stream
            - replay_storage.py - handles sended data
            - worker_storages.py - contains request handlers
        - utils/ - helping stuff, such as greatest common replay, file paths helpers
        - connection.py - represents connection, has logic, which handler to run
        - constants.py - configuration
        - db_conn.py - aiomysql helper for working with MySQL
        - logger.py - logging + bugsnag configuration
        - saver.py - collects additional information about replay and saves it
        - server.py - server component, handles connection, runs stream handler
    - tests - contains tests
    - replays - configurable directory by env. variables for saved replays
    - tmp - configurable directory by env. variables for saving files during upload


## How it works:

Request is handled in `server.py` by `handle_connection` method.
From incoming data the request type is determined by `connection.py`.
Then replay worker (`replay_server/stream/base.py`) is created, connection is given to him and he does all job.
Writer workers are storing data into temporary files, and when the last one is done, he saves data to storage.
Reader workers are sending most common data to connected clients.
When there is no connections, temporary data are destroyed.
During saving replay, server communicates with MySQL database.

#### TODO stuff:

    - no slow down after 5 minutes of stream (3000 ticks slowdown)
    - security problems
        - like disapprove replay when lobby is not active/created
        - replay is sending data for too much long time/stream is too big
