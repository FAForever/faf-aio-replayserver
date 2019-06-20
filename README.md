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

For python packages, just install dependencies in requirements/main.txt and
requirements/test.txt in a virtualenv. Once everything is setup, you can run
tests with:

`python3 -m pytest`

Server launching and configuration
----------------------------------

After installation, you should have `faf_replay_server` in your PATH. Running
it will launch the server. For configuration, see doc/configuration.rst.
