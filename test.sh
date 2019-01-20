#!/usr/bin/env bash

pyclean .
rm -r tmp/*
rm -r replays/*

DATABASE_WRITE_WAIT_TIME=0 TICK_COUNT_TIMEOUT=0 python3.6 -m pytest -rxs `find ./tests/conftest.py ./tests/fixtures/ ./tests/server/ -iname "*.py"` --profile
