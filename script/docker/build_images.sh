#!/bin/bash

# This script builds our Docker images skyrise-(base|build|run) and optionally removes dangling images afterwards. It
# assumes the user to be in the Unix group docker.
#
# The script is configurable via the following parameters:
#   -t/--tag    The tag of the newly built images (default is latest)
#   -p/--prune  Defines whether dangling (i.e., neither used nor tagged) images are removed (default is false)

set -e
cd "$(dirname "${BASH_SOURCE[0]}")/../../"

TAG=latest
PRUNE=false
while [ "$#" -gt 0 ]; do
    case $1 in
        -t|--tag) TAG="$2"; shift ;;
        -p|--prune) PRUNE=true ;;
            *) echo "Unknown parameter: $1"; echo "Usage: $0 [-t|--tag TAG] [-p|--prune]"; exit 1 ;;
    esac
    shift
done

echo "Building images with tag ${TAG}.."
docker build -t skyrise-base:${TAG} --target skyrise-base .
docker build -t skyrise-build:${TAG} --target skyrise-build .
docker build -t skyrise-run:${TAG} --target skyrise-run .

if [ "$PRUNE" = true ]; then
    echo "Removing dangling images.."
    docker image prune -f
fi
