#!/usr/bin/env bash

pyclean .
rm -r tmp/*
rm -r replays/*

python3.6 -m pytest -rxs `find ./tests/ -iname "*.py"`
