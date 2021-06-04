include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include
)

# Sources
set(
    TEST_SOURCES
    benchmark/lib/benchmark_helper_test.cpp
    benchmark/lib/benchmark_integration_test.cpp
    benchmark/lib/benchmark_result_aggregate_test.cpp
    benchmark/lib/benchmark_result_test.cpp
    benchmark/lib/benchmark_runner_test.cpp
    benchmark/lib/compression/compression_test.cpp
    benchmark/lib/data_generation/tpch_test.cpp
    
    lib/monitoring/function_segments_test.cpp
    lib/monitoring/metrics_collector_test.cpp
    lib/monitoring/tracer_test.cpp
    lib/monitoring/monitoring_test_utils.cpp
    lib/monitoring/monitoring_test_utils.hpp

    lib/serialization/schema_serialization_test.cpp

    lib/statistics/manifest_shared.cpp
    lib/statistics/manifest_shared.hpp
    lib/statistics/manifest_reader_test.cpp
    lib/statistics/manifest_writer_test.cpp
    lib/statistics/statistics_collector_test.cpp

    lib/statistics/statistics_collector_test.cpp
    lib/statistics/manifest_shared.cpp
    lib/statistics/manifest_shared.hpp
    lib/statistics/manifest_reader_test.cpp
    lib/statistics/manifest_writer_test.cpp
    lib/statistics/manifest_merger_test.cpp

    lib/storage/backend/abstract_provider.hpp
    lib/storage/backend/filesystem_provider.hpp
    lib/storage/backend/mock_storage.cpp
    lib/storage/backend/mock_storage.hpp
    lib/storage/backend/s3_provider.hpp
    lib/storage/backend/s3_test.cpp
    lib/storage/backend/storage_test.cpp
    lib/storage/backend/stream_test.cpp
    lib/storage/backend/testdata_storage.cpp
    lib/storage/backend/testdata_storage.hpp

    lib/storage/formats/csv_reader_test.cpp
    lib/storage/formats/csv_writer_test.cpp
    lib/storage/formats/orc_reader_test.cpp
    lib/storage/formats/orc_writer_test.cpp

    lib/storage/table/chunk_test.cpp
    lib/storage/table/table_column_definition_test.cpp
    lib/storage/table/table_writer_test.cpp
    lib/storage/table/value_segment_test.cpp

    lib/utils/costs/cost_calculator_test.cpp
    lib/utils/costs/costs_test_utils.cpp
    lib/utils/costs/costs_test_utils.hpp
    lib/utils/costs/pricing_test.cpp
    lib/utils/profiling/function_host_information_test.cpp
    lib/utils/string_test.cpp

    lib/testing/aws_test.hpp

    utils/string_test_utils.cpp
    utils/string_test_utils.hpp
)

# Libraries
set(
    TEST_LIBRARIES
    gtest_main
)

add_executable(skyriseTest ${TEST_SOURCES})

target_link_libraries(skyriseTest skyriseBenchmark skyriseCoordinator skyriseWorker ${TEST_LIBRARIES})

# Copy test files that are used in unit tests
add_custom_command(
    TARGET skyriseTest POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E create_symlink
        ${CMAKE_SOURCE_DIR}/resources
        ${CMAKE_BINARY_DIR}/resources)
