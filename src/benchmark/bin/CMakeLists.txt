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

add_subdirectory(function)
