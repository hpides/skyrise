#!/bin/bash

# This script builds our project inside a Docker container (using the image ${IMAGE_PREFIX}/${IMAGE_NAME}-${IMAGE_DATE}).
# The project directory is mounted into the Docker container, and the output of the build process is stored in the
# subdirectory (BUILD_DIR). The script assumes the user to be in the Unix group docker.
#
# The script is configurable via the following parameters:
#   -b/--build-dir          BUILD_DIR        [default="cmake-build-debug"]           The subdirectory of the project root where output files are stored.
#   -c/--cmake              CMAKE_OPTIONS                                            A string of options that is passed to CMake
#                                                                                    (e.g. '-DONE_OPTION=ON -DOTHER_OPTION=OFF').
#   -d/--date               IMAGE_DATE       [default is defined in image.conf]      The creation date of the Docker image.
#   -f/--cmake-force                                                                 Forced re-run of CMake to ignore CMakeCache.txt files.
#   -k/--ninja-tolerance    NINJA_TOLERANCE  [default="1"]                           The number of failed jobs after which ninja aborts the build.
#   -m/--make-target        MAKE_TARGET      [default="all"]                         The target for make.
#   -p/--prefix             IMAGE_PREFIX     [default is defined in image.conf]      The repository name for the Docker image.
#   -t/--build-type         BUILD_TYPE       [default="Debug"]                       The CMake build type (default is Debug).
#   -v/--verbose                                                                     Activate verbose console output.

set -e
exitWithError() {
    newline="\n\t\t\t\t\t"
    echo "$1"
    echo -e "Usage: script/docker/build_project.sh    [-b|--build-dir        BUILD_DIR      ]" $newline \
                                                     "[-c|--cmake            CMAKE_OPTIONS  ]" $newline \
                                                     "[-d|--date             IMAGE_DATE     ]" $newline \
                                                     "[-f|--cmake-force                     ]" $newline \
                                                     "[-k|--ninja-tolerance  NINJA_TOLERANCE]" $newline \
                                                     "[-m|--make-target      MAKE_TARGET    ]" $newline \
                                                     "[-p|--prefix           IMAGE_PREFIX   ]" $newline \
                                                     "[-t|--build-type       BUILD_TYPE     ]" $newline \
                                                     "[-v|--verbose                         ]"
    exit 1
}

# Sets variables describing the most recent Docker image version
# shellcheck source=script/docker/image.conf
source "$(dirname "$0")"/image.conf

BUILD_DIR="cmake-build-debug"
CMAKE_OPTIONS=''
CMAKE_FORCE="false"
NINJA_TOLERANCE="1"
MAKE_TARGET="all"
BUILD_TYPE="Debug"
VERBOSE="false"

while [ "$#" -gt 0 ]; do
    case $1 in
        -b|--build-dir)         BUILD_DIR="$2";                           shift ;;
        -c|--cmake)             CMAKE_OPTIONS="$2";                       shift ;;
        -d|--date)              IMAGE_DATE="$2";                          shift ;;
        -f|--cmake-force)       CMAKE_FORCE="true";                       ;;
        -k|--ninja-tolerance)   NINJA_TOLERANCE="$2";                     shift ;;
        -m|--make-target)       MAKE_TARGET="$2";                         shift ;;
        -p|--prefix)            IMAGE_PREFIX="$2";                        shift ;;
        -t|--build-type)        BUILD_TYPE="$2";                          shift ;;
        -v|--verbose)           VERBOSE="true";                           ;;
        *)                      exitWithError "Invalid input parameters." ;;
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

SOURCE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../"; pwd)
mkdir -p "${SOURCE_DIR}/${BUILD_DIR}"
CMAKE_COMMAND=''
if [ ! -f "${SOURCE_DIR}/${BUILD_DIR}/CMakeCache.txt" ] || [ "$CMAKE_FORCE" = true ]; then
    CMAKE_COMMAND="cmake .. -GNinja -DCMAKE_C_COMPILER=/usr/bin/clang -DCMAKE_CXX_COMPILER=/usr/bin/clang++ -DCMAKE_BUILD_TYPE=${BUILD_TYPE} ${CMAKE_OPTIONS}; "
fi

PROJECT_MOUNT_POINT=/var/skyrise
BUILD_COMMAND="export CCACHE_DIR=${PROJECT_MOUNT_POINT}/ccache; \
               cd ${PROJECT_MOUNT_POINT}/${BUILD_DIR}; \
               ${CMAKE_COMMAND} \
               ninja-build $MAKE_TARGET -k$NINJA_TOLERANCE -j$NUM_CORES"

USER="$(id -u)"
GROUP="$(id -g)"
# shellcheck disable=SC2153
COMMAND="docker run --rm -it \
                    --user ${USER}:${GROUP} \
                    --volume ${SOURCE_DIR}:${PROJECT_MOUNT_POINT} \
                    ${IMAGE_PREFIX}/${IMAGE_NAME}-${IMAGE_DATE} bash -c \"${BUILD_COMMAND}\""

if [ "$VERBOSE" = true ]; then
    echo "Executing build command: ${COMMAND}"
fi
eval "${COMMAND}"
