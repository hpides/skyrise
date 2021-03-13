#!/bin/bash

# This script builds our project inside a Docker container (using image PREFIX/skyrise:build). The project directory is
# mounted into the Docker container, and the output of the build process is stored in subdirectory (BUILD_DIR). The
# script assumes the user to be in the Unix group docker.
#
# The script is configurable via the following parameters:
#   -b/--build-dir      The subdirectory of the project root where output files are stored (default is cmake-build-debug)
#   -c/--cmake          A string of options that is passed to CMake (e.g. '-DONE_OPTION=ON -DOTHER_OPTION=OFF')
#   -m/--make-target    The target for make (default is all)
#   -p/--prefix         The prefix of the repository name for the Docker image (default is hpiepic)
#   -t/--build-type     The CMake build type (default is Debug)
#   -v/--verbose        Activate verbose console output

set -e
exitWithError() {
    echo "$1"
    echo "Usage: $0 [-b|--build-dir BUILD_DIR] [-c|--cmake CMAKE_OPTIONS] [-m|--make-target MAKE_TARGET] [-p|--prefix PREFIX] [-t|--build-type BUILD_TYPE] [-v|--verbose]"
    exit 1
}

SOURCE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../"; pwd)
BUILD_DIR=cmake-build-debug
CMAKE_OPTIONS=''
CMAKE_FORCE=false
MAKE_TARGET=all
PREFIX=hpiepic
BUILD_TYPE=Debug
VERBOSE=false
while [ "$#" -gt 0 ]; do
    case $1 in
        -b|--build-dir) BUILD_DIR="$2"; shift ;;
        -c|--cmake) CMAKE_OPTIONS="$2"; shift ;;
        -f|--cmake-force) CMAKE_FORCE=true ;;
        -p|--prefix) PREFIX="$2"; shift ;;
        -t|--build-type) BUILD_TYPE="$2"; shift ;;
        -m|--make-target) MAKE_TARGET="$2"; shift ;;
        -v|--verbose) VERBOSE=true ;;
            *) exitWithError "Too many parameters passed." ;;
    esac
    shift
done

if [ "$(uname -s)" = Linux ]; then
    NUM_CORES=$(nproc)
elif [ "$(uname -s)" = Darwin ]; then
    NUM_CORES=$(sysctl -n hw.logicalcpu)
else
    echo "Unsupported operating system: $(uname -s)"
    exit 1
fi

mkdir -p "${SOURCE_DIR}/${BUILD_DIR}"
CMAKE_COMMAND=''
if [ ! -f "${SOURCE_DIR}/${BUILD_DIR}/CMakeCache.txt" ] || [ "${CMAKE_FORCE}" = true ]; then
    CMAKE_COMMAND="cmake .. -GNinja -DCMAKE_C_COMPILER=/usr/bin/clang -DCMAKE_CXX_COMPILER=/usr/bin/clang++ -DCMAKE_BUILD_TYPE=${BUILD_TYPE} ${CMAKE_OPTIONS}; "
fi

PROJECT_MOUNT_POINT=/var/skyrise
BUILD_COMMAND="export CCACHE_DIR=${PROJECT_MOUNT_POINT}/ccache; \
cd ${PROJECT_MOUNT_POINT}/${BUILD_DIR}; \
${CMAKE_COMMAND}\
ninja-build $MAKE_TARGET -j$NUM_CORES"

USER_ID="$(id -u)"
COMMAND="docker run --rm \
--user ${USER_ID} \
--volume ${SOURCE_DIR}:${PROJECT_MOUNT_POINT} \
${PREFIX}/skyrise:build bash -c \"${BUILD_COMMAND}\""

if [ "$VERBOSE" = true ]; then
    echo "Executing build command: ${COMMAND}"
fi
eval ${COMMAND}
