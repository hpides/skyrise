#!/bin/bash

# This script builds our Docker images and optionally removes dangling images afterwards. It assumes the user to be in
# the Unix group docker.
#
# The script is configurable via the following parameters:
#   --prefix    The prefix of the repository name for the images (default is user's name)
#   --prune     Defines whether dangling (i.e., neither used nor tagged) images are removed (default is false)

set -e
exitWithError() {
    echo "$1"
    echo "Usage: $0 [--prefix PREFIX] [--prune]"
    exit 1
}

SOURCE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../"; pwd)
DATE=$(date +'%Y%m%d')
PREFIX=$USER
PRUNE=false
PLATFORM=$(uname -m | cut -c 1-3)

while [ "$#" -gt 0 ]; do
    case $1 in
        --prefix) PREFIX="$2"; shift ;;
        --prune) PRUNE=true ;;
            *) exitWithError "Unknown parameter: $1" ;;
    esac
    shift
done

echo "Building images with repository prefix ${PREFIX}.."
# Build Amazon Linux 2 image
docker buildx build --load --target al2 \
    --tag "${PREFIX}"/skyrise:al2-"${PLATFORM}"-"${DATE}" --tag "${PREFIX}"/skyrise:al2-"${PLATFORM}" "${SOURCE_DIR}"

# Build Ubuntu image
docker buildx build --load --target ubuntu \
    --tag "${PREFIX}"/skyrise:ubuntu-"${PLATFORM}"-"${DATE}" --tag "${PREFIX}"/skyrise:ubuntu-"${PLATFORM}" "${SOURCE_DIR}"

if [ "$PRUNE" = true ]; then
    echo "Removing dangling images.."
    docker image prune -f
    echo "Removing dangling build caches.."
    docker builder prune -f
fi
