#!/usr/bin/env bash
pyclean .
rm -r tmp/*
rm -r replays/*
STREAM_ENDED_TIMEOUT=0 python3.6 -m pytest --cov=replay_server --cov-report=html -rxs `find ./tests/ -iname "*.py"`
