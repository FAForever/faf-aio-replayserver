#!/usr/bin/env bash

SOURCE="${BASH_SOURCE[0]}"
DIR="$( dirname "$SOURCE" )"

PYTHONPATH=$DIR python3.6 $DIR/replay_server/__main__.py
