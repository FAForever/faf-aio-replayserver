Replay server
=============

Read streams from users and stores them into the files on server side.
Also can read stream most common stream to other users.
After the game is end, chooses the biggest common streams and saves them.


Project directory structure:

    - replay_server - contains code
        - __main__.py - starts server
        - replay_parser/ - SCFA replay parser, used for reading replay headers
        - stream/ - stream handlers for saving and serving streams
        - utils/ - helping stuff, such as greatest common replay, file paths helpers
        - connection.py - helping connection stuff
        - constants.py - configuration
        - db_conn.py - aiomysql helper for working with MySQL
        - logger.py - logging + bugsnag configuration
        - saver.py - collects additional information about replay and saves it.
        - server.py - server component, handles connection, executes replay_processor.py
    - tests - contains tests
    - replays - configurable directory by env. variables for saved replays
    - tmp - configurable directory by env. variables for saving files during upload


TODO things:

    1. dockerfile
    2. bugsnag
    3. tests + deployment (https://github.com/FAForever/faf-java-api/blob/develop/.travis.yml) 
        + docker push (https://github.com/FAForever/server/blob/develop/.travis.yml)
    4. Sleep 5 min, before stream starts


