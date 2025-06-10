add_executable(
    systemBenchmark

    ../benchmark_executable.cpp
    ../benchmark_executable.hpp
    benchmark_system.cpp
)

target_link_libraries(
    systemBenchmark

    skyriseBenchmark
)
