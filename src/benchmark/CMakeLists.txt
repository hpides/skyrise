add_executable(
    idleLifetimeBenchmark

    idle_lifetime_benchmark.cpp
    ${SHARED_SOURCES}
)

target_link_libraries(idleLifetimeBenchmark skyriseBenchmark)

add_executable(
    invocationThroughputBenchmark

    invocation_throughput_benchmark.cpp
    ${SHARED_SOURCES}
)

target_link_libraries(invocationThroughputBenchmark skyriseBenchmark)

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

add_executable(
    simpleBenchmark
    network_benchmark.cpp
    network_benchmark.hpp
    network_latency_benchmark.cpp
    network_latency_benchmark.hpp
    network_throughput_benchmark.cpp
    network_throughput_benchmark.hpp
    simple_benchmark.cpp
)

target_link_libraries(simpleBenchmark skyriseBenchmark)

add_subdirectory(bin)
add_subdirectory(lib)
