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
    simpleBenchmark

    simple_benchmark.cpp
)

target_link_libraries(simpleBenchmark skyriseBenchmark)

add_subdirectory(bin)
add_subdirectory(lib)
