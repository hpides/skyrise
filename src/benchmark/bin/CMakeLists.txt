add_executable(
    skyriseBenchmarkFunctionColocation

    benchmark_function_colocation.cpp
)

target_link_libraries(
    skyriseBenchmarkFunctionColocation

    skyriseBenchmark
)

add_executable(
    skyriseBenchmarkFunctionTemperature

    benchmark_function_temperature.cpp
)

target_link_libraries(
    skyriseBenchmarkFunctionTemperature

    skyriseBenchmark
)

add_executable(
    skyriseBenchmarkIdleAvailability

    benchmark_idle_availability.cpp
)

target_link_libraries(
    skyriseBenchmarkIdleAvailability

    skyriseBenchmark
)

add_executable(
    skyriseBenchmarkIdleLifetime

    benchmark_idle_lifetime.cpp
)

target_link_libraries(
    skyriseBenchmarkIdleLifetime

    skyriseBenchmark
)

add_executable(
    skyriseBenchmarkInvocationLatency

    benchmark_invocation_latency.cpp
)

target_link_libraries(
    skyriseBenchmarkInvocationLatency

    skyriseBenchmark
)

add_executable(
    skyriseBenchmarkInvocationThroughput

    benchmark_invocation_throughput.cpp
)

target_link_libraries(
    skyriseBenchmarkInvocationThroughput

    skyriseBenchmark
)

add_executable(
    skyriseBenchmarkNetworkLatency

    benchmark_network_latency.cpp
)

target_link_libraries(
    skyriseBenchmarkNetworkLatency

    skyriseBenchmark
)

add_executable(
    skyriseBenchmarkNetworkThroughput

    benchmark_network_throughput.cpp
)

target_link_libraries(
    skyriseBenchmarkNetworkThroughput

    skyriseBenchmark
)

add_executable(
    skyriseBenchmarkNetworkThroughputParallel

    benchmark_network_throughput_parallel.cpp
)

target_link_libraries(
    skyriseBenchmarkNetworkThroughputParallel

    skyriseBenchmark
)

add_executable(
    skyriseFunctionHostId

        function/function_host_id.cpp
        function/function_host_id.hpp
)

target_link_libraries(
    skyriseFunctionHostId

    PRIVATE
    skyriseWorker
)

aws_lambda_package_target(skyriseFunctionHostId)

add_executable(
    skyriseFunctionMinimal

        function/function_minimal.cpp
)

target_link_libraries(
    skyriseFunctionMinimal

    PRIVATE
    aws-lambda-runtime
)

aws_lambda_package_target(skyriseFunctionMinimal)

add_custom_target(
    sized-functions ALL
    COMMAND ${CMAKE_SOURCE_DIR}/script/benchmark/sized_function_packager.sh ${CMAKE_PACKAGE_OUTPUT_DIRECTORY}
    DEPENDS aws-lambda-package-skyriseFunctionMinimal
)

add_executable(
    skyriseFunctionReadS3

        function/function_read_s3.cpp
        function/function_read_s3.hpp
)

target_link_libraries(
    skyriseFunctionReadS3

    PRIVATE
    aws-cpp-sdk-s3
    skyriseWorker
)

aws_lambda_package_target(skyriseFunctionReadS3)

add_executable(
    skyriseFunctionSimple

        function/function_simple.cpp
        function/function_simple.hpp
)

target_link_libraries(
    skyriseFunctionSimple

    PRIVATE
    skyriseWorker
)

aws_lambda_package_target(skyriseFunctionSimple)

add_executable(
    skyriseFunctionWriteS3

        function/function_write_s3.cpp
        function/function_write_s3.hpp
)

target_link_libraries(
    skyriseFunctionWriteS3

    PRIVATE
    aws-cpp-sdk-s3
    skyriseWorker
)

aws_lambda_package_target(skyriseFunctionWriteS3)
