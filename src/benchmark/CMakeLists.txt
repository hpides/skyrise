add_executable(
    skyriseFunctionMinimal

    function_minimal.cpp
)

target_link_libraries(
    skyriseFunctionMinimal

    PRIVATE
    aws-lambda-runtime
)

# TODO: Employ custom packager that does not include libraries present in AWS Lambda's execution environment.
aws_lambda_package_target(skyriseFunctionMinimal NO_LIBC)

add_executable(
    simpleBenchmark

    simple_benchmark.cpp
    lib/benchmark_config.cpp
    lib/benchmark_config.hpp
    lib/benchmark_runner.cpp
    lib/benchmark_runner.hpp
)

target_link_libraries(
    simpleBenchmark

    PRIVATE
    aws-cpp-sdk-core
    aws-cpp-sdk-iam
    aws-cpp-sdk-lambda
    aws-cpp-sdk-sqs
)

add_subdirectory(bin)
