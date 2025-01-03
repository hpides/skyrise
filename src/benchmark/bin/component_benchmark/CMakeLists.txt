include_directories(
    ${PROJECT_SOURCE_DIR}/third_party/benchmark/include
)

add_executable(
    skyriseComponentBenchmarks

    operator/hash_join_operator_benchmark.cpp
    operator/partition_operator_benchmark.cpp
)

target_compile_options(
    skyriseComponentBenchmarks

    PUBLIC
    -Wno-deprecated-declarations
)

target_link_libraries(
    skyriseComponentBenchmarks

    skyriseBenchmark
    benchmark::benchmark_main
)
