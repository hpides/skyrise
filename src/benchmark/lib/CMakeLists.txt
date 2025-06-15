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
)

set(
    BENCHMARK_LIBRARIES

    aws-cpp-sdk-core
    aws-cpp-sdk-s3
    gtest_main

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
