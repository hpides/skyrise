#!/bin/bash

time ./lambdaStorageBenchmark --function_instance_sizes_mb 7076 --concurrent_instance_counts 100 --repetition_count 10 --after_repetition_delays_min 0 --object_size_kb 1 --object_count 1000 --enable_writes --storage_types kS3 --verbose write-step.json

warming_step=0
for i in {10..50}
do
        warming_step=$((warming_step+1))
        time ./lambdaStorageBenchmark --function_instance_sizes_mb 7076 --concurrent_instance_counts $((i*2)) --repetition_count 10 --after_repetition_delays_min 0 --object_size_kb 1 --object_count 1000 --enable_reads --storage_types kS3 --verbose warming-step.$warming_step.json
done

cooling_step=0
# shellcheck disable=SC2034
for j in {1..36}
do
        cooling_step=$((cooling_step+1))
        sleep 3600
        time ./lambdaStorageBenchmark --function_instance_sizes_mb 7076 --concurrent_instance_counts 100 --repetition_count 2 --after_repetition_delays_min 0 --object_size_kb 1 --object_count 1000 --enable_reads --storage_types kS3 --verbose cooling-step.$cooling_step.json
done
