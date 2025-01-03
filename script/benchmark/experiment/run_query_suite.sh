#!/bin/bash

# shellcheck disable=SC2034
cooling_step=0
for j in {1..96}
do
        sleep 9
        time ./lambdaTpchBenchmark --function_instance_sizes_mb 7076 --concurrent_instance_count 250 --repetition_count 1 --scale_factor 1000 --query 6 test.q6.sf1000.json &>> run.txt
done
