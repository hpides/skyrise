add_executable(
    skyriseBenchmarkFunctionColocation

    benchmark_executable.cpp
    benchmark_executable.hpp
    benchmark_function_colocation.cpp
)

target_link_libraries(
    skyriseBenchmarkFunctionColocation

    skyriseBenchmark
)

add_executable(
    skyriseBenchmarkFunctionWarmUp

    benchmark_executable.cpp
    benchmark_executable.hpp
    benchmark_function_warm_up.cpp
)

target_link_libraries(
    skyriseBenchmarkFunctionWarmUp

    skyriseBenchmark
)

add_executable(
    skyriseBenchmarkFunctionWarmUpContinuous

    benchmark_executable.cpp
    benchmark_executable.hpp
    benchmark_function_warm_up_continuous.cpp
)

target_link_libraries(
    skyriseBenchmarkFunctionWarmUpContinuous

    skyriseBenchmark
)

add_executable(
    skyriseBenchmarkIdleAvailability

    benchmark_executable.cpp
    benchmark_executable.hpp
    benchmark_idle_availability.cpp
)

target_link_libraries(
    skyriseBenchmarkIdleAvailability

    skyriseBenchmark
)

add_executable(
    skyriseBenchmarkIdleLifetime

    benchmark_executable.cpp
    benchmark_executable.hpp
    benchmark_idle_lifetime.cpp
)

target_link_libraries(
    skyriseBenchmarkIdleLifetime

    skyriseBenchmark
)

add_executable(
    skyriseBenchmarkInvocationLatency

    benchmark_executable.cpp
    benchmark_executable.hpp
    benchmark_invocation_latency.cpp
)

target_link_libraries(
    skyriseBenchmarkInvocationLatency

    skyriseBenchmark
)

add_executable(
    skyriseBenchmarkInvocationThroughput

    benchmark_executable.cpp
    benchmark_executable.hpp
    benchmark_invocation_throughput.cpp
)

target_link_libraries(
    skyriseBenchmarkInvocationThroughput

    skyriseBenchmark
)

add_executable(
    skyriseBenchmarkNetworkLatency

    benchmark_executable.cpp
    benchmark_executable.hpp
    benchmark_network_latency.cpp
)

target_link_libraries(
    skyriseBenchmarkNetworkLatency

    skyriseBenchmark
)

add_executable(
    skyriseBenchmarkNetworkThroughput

    benchmark_executable.cpp
    benchmark_executable.hpp
    benchmark_network_throughput.cpp
)

target_link_libraries(
    skyriseBenchmarkNetworkThroughput

    skyriseBenchmark
)

add_executable(
    skyriseBenchmarkNetworkThroughputParallel

    benchmark_executable.cpp
    benchmark_executable.hpp
    benchmark_network_throughput_parallel.cpp
)

target_link_libraries(
    skyriseBenchmarkNetworkThroughputParallel

    skyriseBenchmark
)

add_subdirectory(function)
