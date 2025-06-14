#!/bin/bash

# Sets variables describing the most recent Docker image version
# shellcheck source=script/build/docker/image.conf
# shellcheck disable=SC1094
source "$(dirname "$0")"/image.conf

USER="$(id -u)"
GROUP="$(id -g)"
SOURCE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../" || exit; pwd)
PROJECT_MOUNT_POINT="/tmp/skyrise"

AWS_ACCESS_KEY_ID=$(aws --profile default configure get aws_access_key_id)
AWS_SECRET_ACCESS_KEY=$(aws --profile default configure get aws_secret_access_key)

if [[ "$#" -eq 0 ]]; then
  GTEST_FILTER_FLAGS=""
elif [[ "$#" -eq 1 ]] && [[ $1 == --gtest_filter=* ]]; then
  GTEST_FILTER_FLAGS=$1
else
  echo "Call this script either without parameters or with --gtest_filter=\"..\""
  exit
fi

COMMAND="cd cmake-build-debug && bin/skyriseTest $GTEST_FILTER_FLAGS"

DOCKER_COMMAND="docker run --rm -it \
                           --user ${USER}:${GROUP} \
                           --volume ${SOURCE_DIR}:${PROJECT_MOUNT_POINT} \
                           -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
                           -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
                           ${IMAGE_PREFIX}/${IMAGE_NAME}-${IMAGE_DATE} bash -c \"cd ${PROJECT_MOUNT_POINT} && ${COMMAND}\""

eval "${DOCKER_COMMAND}"
