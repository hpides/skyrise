#!/bin/bash

# This script builds our Docker images skyrise:(base|build|run) and optionally removes dangling images afterwards. It
# assumes the user to be in the Unix group docker.
#
# The script is configurable via the following parameters:
#   --prefix  The prefix of the repository name for the images (default is user's name)
#   --prune   Defines whether dangling (i.e., neither used nor tagged) images are removed (default is false)

set -e
cd "$(dirname "${BASH_SOURCE[0]}")/../../"

PREFIX=$USER
PRUNE=false
while [ "$#" -gt 0 ]; do
    case $1 in
        --prefix) PREFIX="$2"; shift ;;
        --prune) PRUNE=true ;;
            *) echo "Unknown parameter: $1"; echo "Usage: $0 [--prefix PREFIX] [--prune]"; exit 1 ;;
    esac
    shift
done

echo "Building images with repository prefix ${PREFIX}.."
export DOCKER_BUILDKIT=1
docker build --target base --tag ${PREFIX}/skyrise:base .
docker build --target build --tag ${PREFIX}/skyrise:build .
docker build --target run --tag ${PREFIX}/skyrise:run .

if [ "$PRUNE" = true ]; then
    echo "Removing dangling images.."
    docker image prune -f
fi
