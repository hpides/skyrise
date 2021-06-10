#!/bin/bash

## This script runs skyriseTest from inside a Docker container.
## For easy filtering, the script accepts the parameter --gtest_filter=".." and passes it to GoogleTest.

if [[ "$#" -eq 0 ]]; then
  GTEST_FILTER_FLAGS=""
elif [[ "$#" -eq 1 ]] && [[ $1 == --gtest_filter=* ]]; then
  GTEST_FILTER_FLAGS=$1
else
  echo "Call this script either without parameters or with --gtest_filter=\"..\""
  exit
fi

COMMAND="cd /var/skyrise/cmake-build-debug/ && bin/skyriseTest $GTEST_FILTER_FLAGS"

PREFIX="hpiepic"
IMAGE_DATE="20210423"
IMAGE="skyrise:amazonlinux2-${IMAGE_DATE}"

USER="$(id -u)"
GROUP="$(id -g)"
SOURCE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../"; pwd)
PROJECT_MOUNT_POINT="/var/skyrise"

DOCKER_COMMAND="docker run --rm -it \
                           --user ${USER}:${GROUP} \
                           --volume ${SOURCE_DIR}:${PROJECT_MOUNT_POINT} \
                           ${PREFIX}/${IMAGE} bash -c \"${COMMAND}\""

eval ${DOCKER_COMMAND}
