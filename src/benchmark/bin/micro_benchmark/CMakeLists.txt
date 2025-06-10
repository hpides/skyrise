add_subdirectory(function)

add_executable(
    ec2StartupBenchmark

    ../benchmark_executable.cpp
    ../benchmark_executable.hpp
    benchmark_ec2_startup.cpp
)

target_link_libraries(
    ec2StartupBenchmark

    skyriseBenchmark
)

add_executable(
    ec2StorageBenchmark

    ../benchmark_executable.cpp
    ../benchmark_executable.hpp
    benchmark_ec2_storage.cpp
)

target_link_libraries(
    ec2StorageBenchmark

    skyriseBenchmark
)

add_executable(
    lambdaAvailabilityBenchmark

    ../benchmark_executable.cpp
    ../benchmark_executable.hpp
    benchmark_lambda_availability.cpp
)

target_link_libraries(
    lambdaAvailabilityBenchmark

    skyriseBenchmark
)

add_executable(
    lambdaColocationBenchmark

    ../benchmark_executable.cpp
    ../benchmark_executable.hpp
    benchmark_lambda_colocation.cpp
)

target_link_libraries(
    lambdaColocationBenchmark

    skyriseBenchmark
)

add_executable(
    lambdaIdletimeBenchmark

    ../benchmark_executable.cpp
    ../benchmark_executable.hpp
    benchmark_lambda_idletime.cpp
)

target_link_libraries(
    lambdaIdletimeBenchmark

    skyriseBenchmark
)

add_executable(
    lambdaStartupLatencyBenchmark

    ../benchmark_executable.cpp
    ../benchmark_executable.hpp
    benchmark_lambda_startup_latency.cpp
)

target_link_libraries(
    lambdaStartupLatencyBenchmark

    skyriseBenchmark
)

add_executable(
    lambdaStartupThroughputBenchmark

    ../benchmark_executable.cpp
    ../benchmark_executable.hpp
    benchmark_lambda_startup_throughput.cpp
)

target_link_libraries(
    lambdaStartupThroughputBenchmark

    skyriseBenchmark
)

add_executable(
    lambdaStorageBenchmark

    ../benchmark_executable.cpp
    ../benchmark_executable.hpp
    benchmark_lambda_storage.cpp
)

target_link_libraries(
    lambdaStorageBenchmark

    skyriseBenchmark
)

add_executable(
    lambdaWarmupBenchmark

    ../benchmark_executable.cpp
    ../benchmark_executable.hpp
    benchmark_lambda_warmup.cpp
)

target_link_libraries(
    lambdaWarmupBenchmark

    skyriseBenchmark
)

add_executable(
    lambdaWarmupBenchmarkContinuous

    ../benchmark_executable.cpp
    ../benchmark_executable.hpp
    benchmark_lambda_warmup_continuous.cpp
)

target_link_libraries(
    lambdaWarmupBenchmarkContinuous

    skyriseBenchmark
)

add_executable(
    lambdaNetworkBenchmark

    ../benchmark_executable.cpp
    ../benchmark_executable.hpp
    benchmark_lambda_network.cpp
)

target_link_libraries(
    lambdaNetworkBenchmark

    skyriseBenchmark
)
