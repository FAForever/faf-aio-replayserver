#!/usr/bin/env bash

pyclean .
rm -r tmp/*
rm -r replays/*

DATABASE_WRITE_WAIT_TIME=0.1 REPLAY_TIMEOUT=1 python3.6 -m pytest --cov=replay_server  -rxs `find ./tests/fixtures/ ./tests/server/ ./tests/conftest.py -iname "*.py"`
