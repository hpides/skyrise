#!/bin/bash

set -euo pipefail

_10_MB=10240

PACKAGE_DIR=$1
PACKAGE_SIZE=$(du $PACKAGE_DIR/skyriseFuncInvocLat.zip 2> /dev/null | cut -f1)
MINIMAL_SIZE=$((_10_MB - PACKAGE_SIZE))
IS_CHANGED=false

if [ $((_10_MB - MINIMAL_SIZE)) = $PACKAGE_SIZE ]; then
  for SIZE in {10,20,30,40,50,100}; do
    if [ ! -f $PACKAGE_DIR/skyriseFuncInvocLat${SIZE}MB.zip ]; then
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
  dd if=/dev/urandom count=1 bs=${MINIMAL_SIZE}K 2> /dev/null > $PACKAGE_DIR/sized_blob_minimal
else
  touch $PACKAGE_DIR/sized_blob_minimal
fi

# Offsets to create the correct zip size for release build
dd if=/dev/urandom count=1 bs=10236K 2> /dev/null > $PACKAGE_DIR/temp_sized_blob_20MB &
dd if=/dev/urandom count=1 bs=20476K 2> /dev/null > $PACKAGE_DIR/temp_sized_blob_30MB &
dd if=/dev/urandom count=1 bs=30712K 2> /dev/null > $PACKAGE_DIR/temp_sized_blob_40MB &
dd if=/dev/urandom count=2 bs=20476K 2> /dev/null > $PACKAGE_DIR/temp_sized_blob_50MB &
dd if=/dev/urandom count=4 bs=23036K 2> /dev/null > $PACKAGE_DIR/temp_sized_blob_100MB &
wait

for SIZE in {20,30,40,50,100}; do
  cat $PACKAGE_DIR/sized_blob_minimal $PACKAGE_DIR/temp_sized_blob_${SIZE}MB > $PACKAGE_DIR/sized_blob_${SIZE}MB &
done
wait


for SIZE in {10,20,30,40,50,100}; do
  cp $PACKAGE_DIR/skyriseFuncInvocLat.zip $PACKAGE_DIR/skyriseFuncInvocLat${SIZE}MB.zip &
done
wait

zip -q -u $PACKAGE_DIR/skyriseFuncInvocLat10MB.zip $PACKAGE_DIR/sized_blob_minimal &

for SIZE in {20,30,40,50,100}; do
zip -q -u $PACKAGE_DIR/skyriseFuncInvocLat${SIZE}MB.zip $PACKAGE_DIR/sized_blob_${SIZE}MB &
done
wait

rm $PACKAGE_DIR/sized_blob_*
