set(
    BENCHMARK_SOURCES
    benchmark.hpp
    benchmark_config.cpp
    benchmark_config.hpp
    benchmark_helper.cpp
    benchmark_helper.hpp
    benchmark_runner.cpp
    benchmark_runner.hpp
    idle_lifetime_benchmark.cpp
    idle_lifetime_benchmark.hpp
    invocation_throughput_benchmark.cpp
    invocation_throughput_benchmark.hpp
    network_benchmark.cpp
    network_benchmark.hpp
    network_latency_benchmark.cpp
    network_latency_benchmark.hpp
    network_throughput_benchmark.cpp
    network_throughput_benchmark.hpp
)

set(
    BENCHMARK_LIBRARIES
    aws-cpp-sdk-core
    aws-cpp-sdk-iam
    aws-cpp-sdk-lambda
    aws-cpp-sdk-s3
    aws-cpp-sdk-sqs
    skyriseShared
)

add_library(skyriseBenchmark SHARED ${BENCHMARK_SOURCES})

target_link_libraries(
    skyriseBenchmark
    INTERFACE
    skyriseCoordinator
    PUBLIC
    ${BENCHMARK_LIBRARIES}
)
