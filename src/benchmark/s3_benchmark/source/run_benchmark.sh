#!/bin/bash

#
#
#   This script is executing the benchmark with a cross-product of the
#   parameters provided.
#
#

# search space definition
# all keys/objects have to be in the same bucket
bucket="testbucket.aws.educate"
# the keys to run the test against
keys=("KiB1" "KiB10" "KiB100" "KiB256" "MiB1" "MiB10" "MiB100" "GiB1" "GiB10")
# name of the storage in which the objects reside
storageName="S3"
# http methods
httpMethods=("GET" "PUT")
# the lambdas that execute the task and their size in MB
lambda="s3benchgeneric"
sizes=(512 1024 2048 3008) # compare lambada paper
# optimizations
readMitigation=(-1)
writeMitigation=(-1)
doubleWrite=(false)
parallelism=(1)
# debug params
dryRun=false
isLocal=false

# other params
csvHasHeader=true
duration=60

# set-up
touch out.csv
touch tmp.csv

for dw in "${doubleWrite[@]}"; do
    for p in "${parallelism[@]}"; do
        for rm in "${writeMitigation[@]}"; do
            for wm in "${readMitigation[@]}"; do
                for s in "${sizes[@]}"; do
                    for key in "${keys[@]}"; do
                        for http in "${httpMethods[@]}"; do
                            cmd="aws lambda invoke --function-name "$lambda$s" --region us-east-1 --payload '{\"s3Bucket\": \"$bucket\", \"isLocal\":$isLocal, \"s3Key\":\"$key\", \"testDurationSec\":$duration, \"requestType\":\"$http\", \"isDryRun\":$dryRun, \"lambdaSize\":$s, \"storageName\":\"$storageName\", \"readMitigation\":$rm, \"writeMitigation\":$wm, \"nrConcurrentRequests\":$p, \"doubleWrite\":$dw, \"csvHasHeader\":$csvHasHeader}' tmp.csv >tmp.csv"
                            eval $cmd
                            # only first invocation has header
                            if $csvHasHeader; then
                                csvHasHeader=false
                            fi
                            cat tmp.csv >>out.csv
                        done
                    done
                done
            done
        done
    done
done

# clean-up
rm tmp.csv
