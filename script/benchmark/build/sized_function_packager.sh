#!/bin/bash

set -euo pipefail

# Offset to create the correct zip size for release build
_10_MB_OFFSET=$((1024 * 4))
_10_MB=$(((1024 * 1024 * 10) - _10_MB_OFFSET))

PACKAGE_DIR=$1
PACKAGE_SIZE=$(du -b "$PACKAGE_DIR"/skyriseMinimalFunction.zip 2> /dev/null | cut -f1)

MINIMAL_SIZE=$((_10_MB - PACKAGE_SIZE))
IS_CHANGED=false

if [ $((_10_MB - MINIMAL_SIZE)) = "$PACKAGE_SIZE" ]; then

  for SIZE in {10,20,30,40,50,100}; do
    if [ ! -f "$PACKAGE_DIR"/skyriseSizedFunction"${SIZE}"MB.zip ]; then

      IS_CHANGED=true
    fi
  done
else
  IS_CHANGED=true
fi

if [ $IS_CHANGED = false ]; then
  exit 0
fi

if [ $MINIMAL_SIZE -gt 0 ]; then
  dd if=/dev/urandom count=1 bs=${MINIMAL_SIZE} 2> /dev/null > "$PACKAGE_DIR"/sized_blob_minimal
else
  touch "$PACKAGE_DIR"/sized_blob_minimal

fi

# Offsets to create the correct zip size for release build
dd if=/dev/urandom count=1 bs=10240K 2> /dev/null > "$PACKAGE_DIR"/tmp_sized_blob_20MB &
dd if=/dev/urandom count=1 bs=20476K 2> /dev/null > "$PACKAGE_DIR"/tmp_sized_blob_30MB &
dd if=/dev/urandom count=1 bs=30716K 2> /dev/null > "$PACKAGE_DIR"/tmp_sized_blob_40MB &
dd if=/dev/urandom count=2 bs=20476K 2> /dev/null > "$PACKAGE_DIR"/tmp_sized_blob_50MB &
dd if=/dev/urandom count=4 bs=23036K 2> /dev/null > "$PACKAGE_DIR"/tmp_sized_blob_100MB &
wait

for SIZE in {20,30,40,50,100}; do
  cat "$PACKAGE_DIR"/sized_blob_minimal "$PACKAGE_DIR"/tmp_sized_blob_"${SIZE}"MB > "$PACKAGE_DIR"/sized_blob_"${SIZE}"MB &
done
wait


for SIZE in {10,20,30,40,50,100}; do
  cp "$PACKAGE_DIR"/skyriseMinimalFunction.zip "$PACKAGE_DIR"/skyriseSizedFunction"${SIZE}"MB.zip &
done
wait

zip -q -u "$PACKAGE_DIR"/skyriseSizedFunction10MB.zip "$PACKAGE_DIR"/sized_blob_minimal &

for SIZE in {20,30,40,50,100}; do
  zip -q -u "$PACKAGE_DIR"/skyriseSizedFunction"${SIZE}"MB.zip "$PACKAGE_DIR"/sized_blob_"${SIZE}"MB &
done
wait

rm "$PACKAGE_DIR"/sized_blob_*
