#!/usr/bin/env bash

pyclean .
rm -r ./tmp/*
rm -r ./replays/*

SOURCE="${BASH_SOURCE[0]}"
DIR="$( dirname "$SOURCE" )"

REPLAY_TIMEOUT=0 PYTHONPATH=$DIR python3.6 $DIR/replay_server/__main__.py &

SERVER_PID=$!
sleep 1

LIVE_TEST=1 DATABASE_WRITE_WAIT_TIME=0.1 python3.6 -m pytest --cov=replay_server -rxs `find ./tests/conftest.py ./tests/fixtures/ ./tests/live_server/ -iname "*.py"`

kill $SERVER_PID
