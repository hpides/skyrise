add_executable(
    skyriseBenchmarkConsole

    benchmark_console.cpp
)

target_link_libraries(
    skyriseBenchmarkConsole

    skyriseBenchmark
)

add_executable(
    skyriseFunctionHostId

    function_host_id.cpp
)

target_link_libraries(
    skyriseFunctionHostId

    PRIVATE
    skyriseWorker
)

aws_lambda_package_target(skyriseFunctionHostId)

add_executable(
    skyriseFunctionMinimal

    function_minimal.cpp
)

target_link_libraries(
    skyriseFunctionMinimal

    PRIVATE
    skyriseWorker
)

aws_lambda_package_target(skyriseFunctionMinimal)

add_executable(
    skyriseFunctionReadS3

    function_read_s3.cpp
)

target_link_libraries(
    skyriseFunctionReadS3

    PRIVATE
    aws-cpp-sdk-s3
    skyriseWorker
)

aws_lambda_package_target(skyriseFunctionReadS3)

add_executable(
    skyriseFunctionWriteS3

    function_write_s3.cpp
)

target_link_libraries(
    skyriseFunctionWriteS3

    PRIVATE
    aws-cpp-sdk-s3
    skyriseWorker
)

aws_lambda_package_target(skyriseFunctionWriteS3)
