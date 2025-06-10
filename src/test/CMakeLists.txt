include_directories(
    ${PROJECT_SOURCE_DIR}/src/benchmark/lib
    ${PROJECT_SOURCE_DIR}/src/test/lib
    ${PROJECT_SOURCE_DIR}/third_party/googletest/googlemock/include
    ${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include
)

# Sources
set(
    TEST_SOURCES
    benchmark/lib/benchmark_result_aggregate_test.cpp
    benchmark/lib/micro_benchmark/ec2/ec2_benchmark_config_test.cpp
    benchmark/lib/micro_benchmark/ec2/ec2_benchmark_result_test.cpp
    benchmark/lib/micro_benchmark/ec2/ec2_benchmark_runner_test.cpp
    benchmark/lib/micro_benchmark/lambda/lambda_benchmark_result_test.cpp
    benchmark/lib/micro_benchmark/lambda/lambda_benchmark_runner_test.cpp
    benchmark/lib/micro_benchmark/micro_benchmark_test.cpp
    benchmark/lib/system_benchmark/system_benchmark_test.cpp

    lib/compiler/compilation_context_test.cpp
    lib/compiler/physical_query_plan/operator_proxy/aggregate_operator_proxy_test.cpp
    lib/compiler/physical_query_plan/operator_proxy/alias_operator_proxy_test.cpp
    lib/compiler/physical_query_plan/operator_proxy/exchange_operator_proxy_test.cpp
    lib/compiler/physical_query_plan/operator_proxy/export_operator_proxy_test.cpp
    lib/compiler/physical_query_plan/operator_proxy/filter_operator_proxy_test.cpp
    lib/compiler/physical_query_plan/operator_proxy/import_operator_proxy_test.cpp
    lib/compiler/physical_query_plan/operator_proxy/import_options_test.cpp
    lib/compiler/physical_query_plan/operator_proxy/join_operator_proxy_test.cpp
    lib/compiler/physical_query_plan/operator_proxy/limit_operator_proxy_test.cpp
    lib/compiler/physical_query_plan/operator_proxy/operator_proxy_test.cpp
    lib/compiler/physical_query_plan/operator_proxy/partition_operator_proxy_test.cpp
    lib/compiler/physical_query_plan/operator_proxy/projection_operator_proxy_test.cpp
    lib/compiler/physical_query_plan/operator_proxy/sort_operator_proxy_test.cpp
    lib/compiler/physical_query_plan/operator_proxy/union_operator_proxy_test.cpp
    lib/compiler/physical_query_plan/pipeline_fragment_template_test.cpp
    lib/compiler/physical_query_plan/pqp_pipeline_test.cpp
    lib/compiler/physical_query_plan/pqp_serialization_test.cpp
    lib/compiler/physical_query_plan/pqp_utils_test.cpp
    lib/compiler/pqp_generator/pqp_generator_test.cpp

    lib/expression/expression_evaluator_to_result_test.cpp
    lib/expression/expression_serialization_test.cpp

    lib/metadata/schema/glue_catalog_test.cpp
    lib/metadata/schema/glue_utils_test.cpp
    lib/metadata/schema/mock_catalog.cpp
    lib/metadata/schema/mock_catalog.hpp
    lib/metadata/schema/mock_catalog_test.cpp

    lib/monitoring/lambda_segments_analyzer_test.cpp
    lib/monitoring/metrics_collector_test.cpp
    lib/monitoring/tracer_test.cpp
    lib/monitoring/monitoring_test_utils.cpp
    lib/monitoring/monitoring_test_utils.hpp

    lib/operator/aggregate_operator_test.cpp
    lib/operator/alias_operator_test.cpp
    lib/operator/export_operator_test.cpp
    lib/operator/filter_operator_test.cpp
    lib/operator/hash_join_operator_test.cpp
    lib/operator/import_operator_test.cpp
    lib/operator/partition_operator_test.cpp
    lib/operator/projection_operator_test.cpp
    lib/operator/sort_operator_test.cpp

    lib/scheduler/coordinator/lambda_executor_test.cpp
    lib/scheduler/coordinator/mock_executor.hpp
    lib/scheduler/coordinator/mock_executor.cpp
    lib/scheduler/coordinator/pqp_pipeline_scheduler_test.cpp
    
    lib/scheduler/worker/fragment_scheduler_test.cpp

    lib/serialization/schema_serialization_test.cpp

    lib/storage/backend/abstract_provider.hpp
    lib/storage/backend/dynamodb_provider.hpp
    lib/storage/backend/dynamodb_utils_test.cpp
    lib/storage/backend/filesystem_provider.hpp
    lib/storage/backend/mock_storage.cpp
    lib/storage/backend/mock_storage.hpp
    lib/storage/backend/s3_provider.hpp
    lib/storage/backend/s3_test.cpp
    lib/storage/backend/storage_test.cpp
    lib/storage/backend/stream_test.cpp
        lib/storage/backend/test_storage.cpp
        lib/storage/backend/test_storage.hpp
    lib/storage/formats/csv_reader_test.cpp
    lib/storage/formats/csv_writer_test.cpp
    lib/storage/formats/format_reader_test.cpp
    lib/storage/formats/mock_chunk_reader.cpp
    lib/storage/formats/mock_chunk_reader.hpp
    lib/storage/formats/orc_reader_test.cpp
    lib/storage/formats/orc_writer_test.cpp
    lib/storage/formats/parquet_metadata_reader_test.cpp
    lib/storage/formats/parquet_reader_test.cpp
    lib/storage/formats/parquet_writer_test.cpp
    lib/storage/formats/parquet_expression_test.cpp
    lib/storage/io_handle/byte_buffer_test.cpp
    lib/storage/io_handle/mock_object_buffer.cpp
    lib/storage/io_handle/mock_object_buffer.hpp
    lib/storage/io_handle/object_buffer_test.cpp
    lib/storage/table/chunk_reader_test.cpp
    lib/storage/table/chunk_test.cpp
    lib/storage/table/chunk_writer_test.cpp
    lib/storage/table/table_column_definition_test.cpp
    lib/storage/table/table_key_constraint_test.cpp
    lib/storage/table/table_test.cpp
    lib/storage/table/value_segment_test.cpp

    lib/testing/aws_test.hpp
    lib/testing/check_table_equal.cpp
    lib/testing/check_table_equal.hpp
    lib/testing/check_table_equal_test.cpp
    lib/testing/load_table.hpp
    lib/testing/load_table_test.cpp
    lib/testing/testing_assert.hpp

    lib/tool/function_upload_tool_test.cpp
    lib/tool/tool_config_test.cpp

    lib/utils/compression_test.cpp
    lib/utils/costs/cost_calculator_test.cpp
    lib/utils/costs/pricing_test.cpp
    lib/utils/profiling/function_host_information_test.cpp
    lib/utils/signal_handler_test.cpp
    lib/utils/string_test.cpp
    lib/utils/vector_test.cpp

    utils/string_test_utils.cpp
    utils/string_test_utils.hpp
)

# Libraries
set(
    TEST_LIBRARIES
    gtest_main
)

# Some tests are disabled by default and enabled via CMake options
add_definitions(
    -DRUN_SIGNAL_HANDLER_TESTS="${SKYRISE_ENABLE_SIGNAL_HANDLER_TESTS}"
)

add_executable(skyriseTest ${TEST_SOURCES})

target_link_libraries(skyriseTest skyriseBenchmark skyriseCoordinator skyriseWorker ${TEST_LIBRARIES})

# Copy test files that are used in unit tests
add_custom_command(
    TARGET skyriseTest POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E create_symlink
        ${CMAKE_SOURCE_DIR}/resources
        ${CMAKE_BINARY_DIR}/resources)
