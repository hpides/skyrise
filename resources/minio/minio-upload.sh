#!/bin/bash

# usage: ./minio-upload my-bucket my-file.zip

bucket=$1
file=$2

# old host
host=172.17.0.2:9000
s3_key='minio' #minio-access-key'
s3_secret='miniostorage' #minio-secret-key

resource="/${bucket}/${file}"
content_type="application/octet-stream"
date=`date -R`
_signature="PUT\n\n${content_type}\n${date}\n${resource}"
signature=`echo -en ${_signature} | openssl sha1 -hmac ${s3_secret} -binary | base64`

curl -v -X PUT -T "${file}" \
          -H "Date: ${date}" \
          -H "Content-Type: ${content_type}" \
          -H "Host: $host" \
          -H "Authorization: AWS ${s3_key}:${signature}" \
          -H "Expect: 100-continue" \
          http://$host${resource}
          
