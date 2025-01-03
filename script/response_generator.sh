#!/bin/bash
sync;
response="HTTP/1.1 200
lambda-runtime-aws-request-id:$RANDOM
Connection: Keep-Alive
Content-Type: application/json

$1" # Payload goes into the body.

if [ "$(wc -l < "function_out.txt")" -le 12 ]; then
    echo -e "$response";
    else echo -e "HTTP/1.1 404"
fi
