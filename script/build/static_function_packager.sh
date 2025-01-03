#!/bin/bash

set -euo pipefail

print_help() {
    echo -e "Usage: $0 [-s] <binary name>\n"
    echo -e " -s    Run strip on executable\n"
}

RUN_STRIP=""

if [ $# -ge 1 ] && [ "$1" = "-s" ]; then
    RUN_STRIP=true
    shift
fi

if [ $# -lt 1 ]; then
    echo -e "Error: missing arguments\n"
    print_help
    exit 1
fi

PKG_BIN_PATH=$1
PKG_BIN_FILENAME=$(basename "$PKG_BIN_PATH")
PKG_DIR=tmp

mkdir -p "$PKG_DIR"
cp "$PKG_BIN_PATH" "$PKG_DIR/bootstrap"

if [ -n "$RUN_STRIP" ]; then
    strip --strip-all "$PKG_DIR/bootstrap"
fi

pushd "$PKG_DIR" > /dev/null
zip "$PKG_BIN_FILENAME".zip bootstrap
ORIGIN_DIR=$(dirs -l +1)
mv "$PKG_BIN_FILENAME".zip "$ORIGIN_DIR"
popd > /dev/null
rm -r "$PKG_DIR"
echo "Created cloud function package $ORIGIN_DIR/$PKG_BIN_FILENAME.zip"
