#!/bin/bash

# This script is an extended version of the bootstrap script generated for the AWS Lambda C++ runtime (in third_party/
# aws-lambda-cpp/packaging/packager). It runs a provided cloud function locally, assuming Lambda's execution environment.
# Thereby, it handles C++ cloud function artifacts that depend on either Lambda's own version of libc or a custom version
# from another Linux distribution (than Amazon Linux 1). It can optionally run the cloud function in GDB for debugging.
#
# We call this script from within a local Docker container that runs our skyrise:run image (specified in Dockerfile).
# This image simulates Lambda's execution environment via docker-lambda (https://github.com/lambci/docker-lambda), and
# thus allows for testing and debugging as in a cloud deployment.
#
# The script is configurable via the following environment variables:
#   SKYRISE_ARTIFACT            The cloud function executable to run
#   SKYRISE_ARTIFACT_INCL_LIBC  Defines whether SKYRISE_ARTIFACT depends on a custom libc (default is false)
#   SKYRISE_DEBUG               Defines whether SKYRISE_ARTIFACT is run in GDB (default is false)
#   SKYRISE_DEBUG_IP            The IP of GDB server, if SKYRISE_DBUG is true
#   SKYRISE_DEBUG_PORT          The port of GDB server, if SKYRISE_DBUG is true (default is 2159)

SKYRISE_ARTIFACT_INCL_LIBC=${SKYRISE_ARTIFACT_INCL_LIBC:-false}
SKYRISE_DEBUG=${SKYRISE_DEBUG:-false}
SKYRISE_DEBUG_PORT=${SKYRISE_DEBUG_PORT:-"2159"}

set -euo pipefail
export AWS_EXECUTION_ENV=lambda-cpp
if [ -z "$SKYRISE_ARTIFACT" ] ; then
    echo "Error: SKYRISE_ARTIFACT is required but not set"
    exit 1
fi
if [ "$SKYRISE_ARTIFACT_INCL_LIBC" = false ]; then
    echo "${SKYRISE_ARTIFACT} does not depend on custom libc"
    # LD_LIBRARY_PATH additionally contains /lib64, /usr/lib64, /var/runtime and /var/task
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$LAMBDA_TASK_ROOT/lib:$LAMBDA_TASK_ROOT/lib/lib64
    command="$LAMBDA_TASK_ROOT/bin/$SKYRISE_ARTIFACT ${_HANDLER}"
else
    echo "${SKYRISE_ARTIFACT} depends on custom libc"
    command="$LAMBDA_TASK_ROOT/lib/ld-linux-x86-64.so.2 --library-path $LAMBDA_TASK_ROOT/lib $LAMBDA_TASK_ROOT/bin/$SKYRISE_ARTIFACT ${_HANDLER}"
fi
if [ "$SKYRISE_DEBUG" = false ]; then
    echo "Executing ${SKYRISE_ARTIFACT} without gdbserver"
else
    if [ -z "$SKYRISE_DEBUG_IP" ] ; then
        echo "Error: SKYRISE_DEBUG_IP is required but not set"
        exit 1
    fi
    echo "Executing ${SKYRISE_ARTIFACT} with gdbserver on ${SKYRISE_DEBUG_IP}:${SKYRISE_DEBUG_PORT}"
    command="gdbserver ${SKYRISE_DEBUG_IP}:${SKYRISE_DEBUG_PORT} $command"
fi
exec $command
