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
    skyriseFunctionReadWriteS3
    function_read_write_s3.cpp
)

target_link_libraries(
    skyriseFunctionReadWriteS3
    PRIVATE
    aws-cpp-sdk-s3
    skyriseWorker
)

aws_lambda_package_target(skyriseFunctionReadWriteS3)

add_executable(
    simpleBenchmark
    network_latency_benchmark.cpp
    network_latency_benchmark.hpp
    simple_benchmark.cpp
)

target_link_libraries(simpleBenchmark skyriseBenchmark)

add_subdirectory(bin)
add_subdirectory(lib)
