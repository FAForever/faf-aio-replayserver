#!/usr/bin/env bash
pyclean .
rm -r tmp/*
rm -r replays/*
STREAM_ENDED_TIMEOUT=0 python3.6 -m pytest -rxs `find ./tests/ -iname "*.py"`
