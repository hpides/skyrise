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
)

target_link_libraries(simpleBenchmark skyriseBenchmark)

add_subdirectory(bin)
add_subdirectory(lib)
