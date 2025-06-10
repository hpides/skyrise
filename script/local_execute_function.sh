#!/bin/bash
# Run this script to execute a function locally, i.e., ./local_execute_function.sh function 'payload'. Example:
# ./local_execute_function.sh [relative_path]/skyriseStorageIoFunction '{"s3_bucket_name":"","object_keys":[""],"operation_type":"kRead"}'
echo "Execute function locally with name: $1"
echo "Function payload: $2"

DEPENDENCIES=("nc" "lsof")

# Check if nc and lsof are available.
for package in "${DEPENDENCIES[@]}"; do
    if command -v "$package" >/dev/null 2>&1; then
        echo "Package $package is already installed."
    else
        echo "Package $package is not installed. Installing.";
        yum install -y "$package";
    fi
done

export AWS_LAMBDA_RUNTIME_API=localhost:8080;
(nc -l -k -o function_out.txt -p 8080 -c "bash response_generator.sh '$2'" &);
# shellcheck disable=SC2086
./$1;
sleep 3;
sync; # Ensure everything gets written to function_out.txt before killing the nc process.
kill "$(/usr/bin/lsof -ti :8080)";
export AWS_LAMBDA_RUNTIME_API=;
