#!/bin/bash

# This script builds our project inside a Docker container (using image PREFIX/skyrise:build). The project directory is
# mounted into the Docker container, and the output of the build process is stored in subdirectory (BUILD_DIR). The
# script assumes the user to be in the Unix group docker.
#
# The script is configurable via the following parameters:
#   -b/--build-dir  The subdirectory of the project root where output files are stored (default is cmake-build-debug)
#   -c/--cmake      A string of options that is passed to CMake (e.g. '-DONE_OPTION=ON -DOTHER_OPTION=OFF')
#   -p/--prefix     The prefix of the repository name for the Docker image (default is user's name)
#   -t/--build-type The CMake build type (default is Debug)
#   -v/--verbose    Activate verbose console output

set -e
exitWithError() {
    echo "$1";
    echo "Usage: $0 [-b|--build-dir BUILD_DIR] [-c|--cmake CMAKE_OPTIONS] [-p|--prefix PREFIX] [-t|--build-type BUILD_TYPE] [-v|--verbose]";
    exit 1;
}

SOURCE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../"; pwd)
BUILD_DIR=cmake-build-debug
CMAKE_OPTIONS=''
PREFIX=$USER
BUILD_TYPE=Debug
VERBOSE=false
while [ "$#" -gt 0 ]; do
    case $1 in
        -b|--build-dir) BUILD_DIR="$2"; shift ;;
        -c|--cmake) CMAKE_OPTIONS="$2" shift ;;
        -p|--prefix) PREFIX="$2"; shift ;;
        -t|--build-type) BUILD_TYPE="$2"; shift ;;
        -v|--verbose) VERBOSE=true ;;
            *) exitWithError "Too many parameters passed." ;;
    esac
    shift
done

mkdir -p "${SOURCE_DIR}/${BUILD_DIR}"

if [ "$(uname -s)" = Linux ]; then
    NUM_CORES=$(nproc)
elif [ "$(uname -s)" = Darwin ]; then
    NUM_CORES=$(sysctl -n hw.logicalcpu)
else
    echo "Unsupported operating system: $(uname -s)";
    exit 1;
fi

USER_ID="$(id -u)"
BUILD_COMMAND="cd /var/skyrise/${BUILD_DIR}; \
cmake .. -DCMAKE_C_COMPILER=/usr/bin/clang -DCMAKE_CXX_COMPILER=/usr/bin/clang++ -DCMAKE_BUILD_TYPE=${BUILD_TYPE} ${CMAKE_OPTIONS}; \
make all -j$NUM_CORES"

COMMAND="docker run --rm \
--user ${USER_ID} \
--volume ${SOURCE_DIR}:/var/skyrise \
${PREFIX}/skyrise:build bash -c \"${BUILD_COMMAND}\""

if [ "$VERBOSE" = true ]; then
    echo "Executing build command: ${COMMAND}"
fi
eval ${COMMAND}
