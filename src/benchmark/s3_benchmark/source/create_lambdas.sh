#!/bin/bash

#
#
#   This script create Lambdas with with different sizes (defined in sizes array).
#   Lambdas are based on the artifact.zip file.
#
#

lambda=("s3benchgeneric")
sizes=(512 1024 2048 3008) # compare lambada paper
timeout=120

for s in "${sizes[@]}"; do
    aws lambda create-function --function-name $lambda$s --role arn:aws:iam::667018974679:role/lambda-cpp-demo --runtime provided --timeout $timeout --memory-size $s --zip-file fileb://build/artifact.zip --handler hello
done
