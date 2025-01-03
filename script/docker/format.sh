#!/bin/bash

## This script formats all code using the following command from inside a Docker container.
COMMAND="python3 script/build/run_clang_format.py --clang_format_binary clang-format --source_dir src --fix"

# Sets variables describing the most recent Docker image version
# shellcheck source=script/docker/image.conf
source "$(dirname "$0")"/image.conf

USER="$(id -u)"
GROUP="$(id -g)"
SOURCE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../" || exit; pwd)
PROJECT_MOUNT_POINT="/var/skyrise"

DOCKER_COMMAND="docker run --rm -it \
                           --user ${USER}:${GROUP} \
                           --volume ${SOURCE_DIR}:${PROJECT_MOUNT_POINT} \
                           ${IMAGE_PREFIX}/${IMAGE_NAME}-${IMAGE_DATE} bash -c \"cd ${PROJECT_MOUNT_POINT} && ${COMMAND}\""

eval "${DOCKER_COMMAND}"
