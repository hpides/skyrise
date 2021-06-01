#!/bin/bash

## This script formats all code using the following command from inside a Docker container.
COMMAND="python3 script/run_clang_format.py --clang_format_binary clang-format --source_dir src --fix"

PREFIX="hpiepic"
IMAGE="skyrise:build"

USER_ID="$(id -u)"
SOURCE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../"; pwd)
PROJECT_MOUNT_POINT="/var/skyrise"

DOCKER_COMMAND="docker run --rm -it \
                           --user ${USER_ID} \
                           --volume ${SOURCE_DIR}:${PROJECT_MOUNT_POINT} \
                           ${PREFIX}/${IMAGE} bash -c \"cd ${PROJECT_MOUNT_POINT} && ${COMMAND}\""

eval ${DOCKER_COMMAND}
