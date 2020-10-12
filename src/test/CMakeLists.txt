include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include
)

# Sources
set(
    TEST_SOURCES
    benchmark/lib/utils/benchmark_helper_test.cpp

    lib/storage/types/chunk_test.cpp
    lib/storage/types/table_column_definition_test.cpp
    lib/storage/types/value_segment_test.cpp

    lib/utils/costs/cost_calculator_test.cpp
    lib/utils/costs/costs_test_utils.cpp
    lib/utils/costs/costs_test_utils.hpp
    lib/utils/costs/pricing_test.cpp
    lib/utils/profiling/function_host_information_test.cpp
)

# Libraries
set(
    TEST_LIBRARIES
    gtest_main
)

add_executable(skyriseTest ${TEST_SOURCES})

target_link_libraries(skyriseTest skyriseBenchmark skyriseCoordinator skyriseWorker ${TEST_LIBRARIES})
