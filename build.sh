#!/usr/bin/env bash

VERSION=`grep "LABEL version" Dockerfile | cut -d'"' -f2`
IMAGE_VERSION="faf-aio-replayserver:$VERSION"

docker build --rm=true --tag=$IMAGE_VERSION .
