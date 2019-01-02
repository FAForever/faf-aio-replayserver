#!/usr/bin/env bash

VERSION=`grep "LABEL version" Dockerfile | cut -d'"' -f2`
IMAGE_VERSION="faforever/faf-aio-replayserver:$VERSION"

docker stop faf-aio-replayserver
docker rm faf-aio-replayserver
docker run -d --restart=always --name faf-aio-replayserver -p 15000:15000 $IMAGE_VERSION
echo "Container started $IMAGE_VERSION"
