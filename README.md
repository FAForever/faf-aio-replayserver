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

`sphinx-build -b html ./doc ./doc_html`

Documentation covers both server requirements and architecture.

Dev environment and testing
---------------------------

You'll need to setup faf-db for most work requiring a database. See
instructions at https://github.com/FAForever/db and .travis.yml for details on
how to start the container and populate it with test data. Once you setup
faf-db, run the setup\_db.py script to populate it with test data:

`env FAF_STACK_DB_IP=127.0.0.1 python3 tests/db_setup.py`

For python packages, just install dependencies in requirements/main.txt and
requirements/test.txt in a virtualenv. Once everything is setup, you can run
tests with:

`env FAF_STACK_DB_IP=127.0.0.1 python3 python3 -m pytest`

Server launching and configuration
----------------------------------

You can install the replay server to your virtual environment with:

`pip3 install .`

Before you can run it, you will need to set up some configuration (see
doc/configuration.rst for details). To configure using a YAML file, copy the
`example_config.yml` and edit the database and vault path options. You should
now be able to start the server with:

`env RS_CONFIG_FILE=config.yml faf_replay_server`
