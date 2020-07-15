add_executable(
    skyriseMicroBenchmarks

    network_benchmark.cpp
)

target_link_libraries(
    skyriseMicroBenchmarks

    PRIVATE
    aws-lambda-runtime
    aws-cpp-sdk-s3
    aws-cpp-sdk-core
)

aws_lambda_package_target(skyriseMicroBenchmarks NO_LIBC)
