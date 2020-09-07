set(
    BENCHMARK_SOURCES
    benchmark_config.cpp
    benchmark_config.hpp
    benchmark_runner.cpp
    benchmark_runner.hpp
    utils/benchmark_helper.cpp
    utils/benchmark_helper.hpp
    ${SHARED_SOURCES}
)

set(
    BENCHMARK_LIBRARIES
    aws-cpp-sdk-core
    aws-cpp-sdk-iam
    aws-cpp-sdk-lambda
    aws-cpp-sdk-s3
    aws-cpp-sdk-sqs
)

add_library(skyriseBenchmark SHARED ${BENCHMARK_SOURCES})

target_link_libraries(skyriseBenchmark PUBLIC ${BENCHMARK_LIBRARIES})
