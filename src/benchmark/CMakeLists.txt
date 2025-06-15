include_directories(
    ${PROJECT_SOURCE_DIR}/src/benchmark/bin
    ${PROJECT_SOURCE_DIR}/src/benchmark/lib
)

add_subdirectory(bin)
add_subdirectory(lib)

add_library(skyriseBenchmarkConfig INTERFACE)
target_sources(skyriseBenchmarkConfig
    INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/abstract_benchmark_config.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/benchmark_output.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/benchmark_result.hpp
)

target_include_directories(skyriseBenchmarkConfig 
    INTERFACE 
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/third_party/CLI11/include
)
