#!/usr/bin/env bash

pyclean .
rm -r tmp/*
rm -r replays/*

REPLAY_TIMEOUT=1 python3.6 -m pytest -rxs `find ./tests/ -iname "*.py"`
