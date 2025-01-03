set(
    BENCHMARK_SOURCES

    abstract_benchmark_config.cpp
    abstract_benchmark_config.hpp
    abstract_benchmark_result.hpp
    abstract_benchmark_runner.hpp
    abstract_benchmark.hpp
    benchmark_output.cpp
    benchmark_output.hpp
    benchmark_result_aggregate.cpp
    benchmark_result_aggregate.hpp
    component_benchmark/synthetic_table_generator.cpp
    component_benchmark/synthetic_table_generator.hpp
    micro_benchmark/micro_benchmark_utils.cpp
    micro_benchmark/micro_benchmark_utils.hpp
    micro_benchmark/ec2/ec2_benchmark_config.cpp
    micro_benchmark/ec2/ec2_benchmark_config.hpp
    micro_benchmark/ec2/ec2_benchmark_output.cpp
    micro_benchmark/ec2/ec2_benchmark_output.hpp
    micro_benchmark/ec2/ec2_benchmark_result.cpp
    micro_benchmark/ec2/ec2_benchmark_result.hpp
    micro_benchmark/ec2/ec2_benchmark_runner.cpp
    micro_benchmark/ec2/ec2_benchmark_runner.hpp
    micro_benchmark/ec2/ec2_benchmark_types.cpp
    micro_benchmark/ec2/ec2_benchmark_types.hpp
    micro_benchmark/ec2/ec2_startup_benchmark.cpp
    micro_benchmark/ec2/ec2_startup_benchmark.hpp
    micro_benchmark/ec2/ec2_storage_benchmark.cpp
    micro_benchmark/ec2/ec2_storage_benchmark.hpp
    micro_benchmark/lambda/lambda_availability_benchmark.cpp
    micro_benchmark/lambda/lambda_availability_benchmark.hpp
    micro_benchmark/lambda/lambda_benchmark.cpp
    micro_benchmark/lambda/lambda_benchmark.hpp
    micro_benchmark/lambda/lambda_benchmark_config.cpp
    micro_benchmark/lambda/lambda_benchmark_config.hpp
    micro_benchmark/lambda/lambda_benchmark_output.cpp
    micro_benchmark/lambda/lambda_benchmark_output.hpp
    micro_benchmark/lambda/lambda_benchmark_result.cpp
    micro_benchmark/lambda/lambda_benchmark_result.hpp
    micro_benchmark/lambda/lambda_benchmark_runner.cpp
    micro_benchmark/lambda/lambda_benchmark_runner.hpp
    micro_benchmark/lambda/lambda_benchmark_types.hpp
    micro_benchmark/lambda/lambda_colocation_benchmark.cpp
    micro_benchmark/lambda/lambda_colocation_benchmark.hpp
    micro_benchmark/lambda/lambda_idletime_benchmark.cpp
    micro_benchmark/lambda/lambda_idletime_benchmark.hpp
    micro_benchmark/lambda/lambda_invocation_config.hpp
    micro_benchmark/lambda/lambda_network_benchmark.cpp
    micro_benchmark/lambda/lambda_network_benchmark.hpp
    micro_benchmark/lambda/lambda_result_log.cpp
    micro_benchmark/lambda/lambda_result_log.hpp
    micro_benchmark/lambda/lambda_startup_latency_benchmark.hpp
    micro_benchmark/lambda/lambda_startup_latency_benchmark.cpp
    micro_benchmark/lambda/lambda_startup_throughput_benchmark.cpp
    micro_benchmark/lambda/lambda_startup_throughput_benchmark.hpp
    micro_benchmark/lambda/lambda_storage_benchmark.cpp
    micro_benchmark/lambda/lambda_storage_benchmark.hpp
    micro_benchmark/lambda/lambda_warmup_benchmark.cpp
    micro_benchmark/lambda/lambda_warmup_benchmark.hpp
    micro_benchmark/lambda/lambda_warmup_continuous_benchmark.cpp
    micro_benchmark/lambda/lambda_warmup_continuous_benchmark.hpp
    system_benchmark/system_benchmark_config.cpp
    system_benchmark/system_benchmark_config.hpp
    system_benchmark/system_benchmark_result.cpp
    system_benchmark/system_benchmark_result.hpp
    system_benchmark/system_benchmark_runner.cpp
    system_benchmark/system_benchmark_runner.hpp
    system_benchmark/system_benchmark_utils.cpp
    system_benchmark/system_benchmark_utils.hpp
    system_benchmark/system_benchmark.cpp
    system_benchmark/system_benchmark.hpp
)

set(
    BENCHMARK_LIBRARIES

    aws-cpp-sdk-core
    aws-cpp-sdk-ec2
    aws-cpp-sdk-iam
    aws-cpp-sdk-lambda
    aws-cpp-sdk-s3
    aws-cpp-sdk-sqs
    aws-cpp-sdk-xray
    gtest_main
    ssh

    skyriseShared
)

add_library(skyriseBenchmark STATIC ${BENCHMARK_SOURCES})

target_link_libraries(
    skyriseBenchmark

    INTERFACE
    skyriseCoordinator
    PUBLIC
    ${BENCHMARK_LIBRARIES}
)

# We want to link against the static library.
find_library(LIBIPERF libiperf.a REQUIRED)

add_library(
    skyriseBenchmarkUtils

    STATIC
    utils/iperf_wrapper.cpp
    utils/iperf_wrapper.hpp
    utils/synchronize_instances.cpp
    utils/synchronize_instances.hpp
)

target_link_libraries(skyriseBenchmarkUtils PUBLIC ${LIBIPERF})
