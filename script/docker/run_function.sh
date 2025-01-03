#!/bin/bash

# This script runs the specified cloud function FUNCTION inside a Docker container (using image PREFIX/skyrise:run) and
# passes the arguments specified in ARGUMENTS (as a JSON string) to it. The script assumes a subdirectory of the project
# (BUILD_DIR), which must contain the compiled function in another subdirectory /bin/, to be mounted into the container.
# It further assumes the user to be in the Unix group docker.
#
# The script is configurable via the following parameters:
#   FUNCTION        The name of the function
#   -a/--arguments  The arguments that are passed to the function as a JSON string
#   -b/--build-dir  The subdirectory of the project root where the compiled function is stored under subdirectory /bin/
# (default is cmake-build-debug)
#   -d/--debug      If set, the function is executed with gdbserver (default is false)
#   -I/--debug-ip   The IP of gdbserver, if debug is set (default is '127.0.0.1')
#   -P/--debug-port The port of gdbserver, if debug is set (default is 2159)
#   -p/--prefix     The prefix of the repository name for the Docker image (default is user's name)
#   -v/--verbose    Activate verbose console output

set -e
exitWithError() {
    echo "$1"
    echo "Usage: $0 FUNCTION [-a|--arguments ARGUMENTS] [-b|--build-dir BUILD_DIR] [-d/--debug DEBUG]" \
    "[-I/--debug-ip DEBUG_IP] [-P/--debug-port DEBUG_PORT] [-p|--prefix PREFIX] [-v|--verbose] "
    exit 1
}

# Default variables
SOURCE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../"; pwd)
ARGUMENTS='{}'
BUILD_DIR=cmake-build-debug
DEBUG=false
DEBUG_IP='127.0.0.1'
DEBUG_PORT=2159
PREFIX=$USER
VERBOSE=false

# Sets ACCESS_KEY, ACCESS_KEY_SOURCE, SECRET_KEY and SECRET_KEY_SOURCE
# shellcheck source=script/docker/get_aws_credentials.sh
source "$(dirname "${BASH_SOURCE[0]}")"/get_aws_credentials.sh

while [ "$#" -gt 0 ]; do
    case $1 in
        -a|--arguments) ARGUMENTS="$2"; shift ;;
        -b|--build-dir) BUILD_DIR="$2"; shift ;;
        -d|--debug) DEBUG=true ;;
        -I|--debug-ip) DEBUG_IP="$2"; shift ;;
        -P|--debug-port) DEBUG_PORT="$2"; shift ;;
        -p|--prefix) PREFIX="$2"; shift ;;
        -v|--verbose) VERBOSE=true ;;
            *) if [ -z "$FUNCTION" ]; then FUNCTION="$1"; else exitWithError "Too many arguments passed."; fi ;;
    esac
    shift
done

if [ -z "${FUNCTION}" ]; then
    exitWithError "Missing argument: FUNCTION"
fi
if [ -z "$(ls "${BUILD_DIR}"/bin/*"${FUNCTION}"*)" ]; then
    exitWithError "Function ${BUILD_DIR}/bin/${FUNCTION} does not exist"
fi

if [ "$DEBUG" = false ]; then
    DEBUG_ENV=''
else
    DEBUG_ENV="--env SKYRISE_DEBUG=${DEBUG} --env SKYRISE_DEBUG_IP=${DEBUG_IP} --env SKYRISE_DEBUG_PORT=${DEBUG_PORT} \
--publish ${DEBUG_IP}:${DEBUG_PORT}:${DEBUG_PORT}/tcp --cap-add sys_ptrace --security-opt seccomp=unconfined "
fi

COMMAND="docker run --rm \
--volume ${SOURCE_DIR}/${BUILD_DIR}:/var/task \
--env AWS_ACCESS_KEY_ID=${ACCESS_KEY} \
--env AWS_SECRET_ACCESS_KEY=${SECRET_KEY} \
--env AWS_LAMBDA_EVENT_BODY=${ARGUMENTS} \
--env SKYRISE_ARTIFACT=${FUNCTION} \
${DEBUG_ENV} \
${PREFIX}/skyrise:run"

echo "Running function ${FUNCTION} with arguments ${ARGUMENTS}.."
echo "AWS Access Key ID    : ${ACCESS_KEY_SOURCE}"
echo "AWS Secret Access Key: ${SECRET_KEY_SOURCE}"
if [ "$DEBUG" = true ]; then
    echo "Debugger Server IP   : ${DEBUG_IP}"
    echo "Debugger Server Port : ${DEBUG_PORT}"
fi

if [ "$VERBOSE" = true ]; then
    eval "${COMMAND}"
else
    eval "${COMMAND}" 2> /dev/null
fi
