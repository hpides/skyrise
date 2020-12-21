include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include
)

# Sources
set(
    TEST_SOURCES
    benchmark/lib/benchmark_helper_test.cpp
    benchmark/lib/benchmark_result_aggregate_test.cpp

    lib/storage/backend/abstract_provider.hpp
    lib/storage/backend/filesystem_provider.hpp
    lib/storage/backend/mock_storage.cpp
    lib/storage/backend/mock_storage.hpp
    lib/storage/backend/s3_provider.hpp
    lib/storage/backend/storage_test.cpp
    
    lib/storage/formats/csv_test.cpp
    lib/storage/formats/orc_test.cpp
    
    lib/storage/table/table_test.cpp

    lib/storage/types/chunk_test.cpp
    lib/storage/types/table_column_definition_test.cpp
    lib/storage/types/value_segment_test.cpp

    lib/utils/costs/cost_calculator_test.cpp
    lib/utils/costs/costs_test_utils.cpp
    lib/utils/costs/costs_test_utils.hpp
    lib/utils/costs/pricing_test.cpp
    lib/utils/profiling/function_host_information_test.cpp

    lib/testing/aws_test.hpp
)

# Libraries
set(
    TEST_LIBRARIES
    gtest_main
)

add_executable(skyriseTest ${TEST_SOURCES})

target_link_libraries(skyriseTest skyriseBenchmark skyriseCoordinator skyriseWorker ${TEST_LIBRARIES})
