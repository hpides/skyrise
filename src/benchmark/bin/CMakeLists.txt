add_executable(
    skyriseBenchmark

    benchmark_console.cpp
)

target_link_libraries(
    skyriseBenchmark

    tpch-dbgen
)
