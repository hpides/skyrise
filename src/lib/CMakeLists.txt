set(PRE_CONFIGURE_FILE "utils/git_metadata.cpp.in")
set(PRE_CONFIGURE_FILE "utils/git_metadata.cpp.in")
set(POST_CONFIGURE_FILE "utils/git_metadata.cpp")
set(GIT_FAIL_IF_NONZERO_EXIT FALSE)
include(${PROJECT_SOURCE_DIR}/third_party/cmake-git-version-tracking/git_watcher.cmake)

set(
    COORDINATOR_COMPILER_SOURCES

    compiler/compilation_context.cpp
    compiler/compilation_context.hpp
    compiler/logical_query_plan/functional_dependency.cpp
    compiler/logical_query_plan/functional_dependency.hpp
)

set(
    COORDINATOR_METADATA_SOURCES

    metadata/schema/glue_catalog.cpp
    metadata/schema/glue_catalog.hpp
    metadata/schema/glue_utils.cpp
    metadata/schema/glue_utils.hpp
)

set(
    SHARED_COMPILER_SOURCES

    compiler/abstract_compiler.cpp
    compiler/abstract_compiler.hpp
    compiler/abstract_plan_node.hpp
    compiler/enable_make_for_plan_node.hpp
    compiler/physical_query_plan/operator_proxy/abstract_operator_proxy.cpp
    compiler/physical_query_plan/operator_proxy/abstract_operator_proxy.hpp
    compiler/physical_query_plan/operator_proxy/aggregate_operator_proxy.cpp
    compiler/physical_query_plan/operator_proxy/aggregate_operator_proxy.hpp
    compiler/physical_query_plan/operator_proxy/alias_operator_proxy.cpp
    compiler/physical_query_plan/operator_proxy/alias_operator_proxy.hpp
    compiler/physical_query_plan/operator_proxy/exchange_operator_proxy.cpp
    compiler/physical_query_plan/operator_proxy/exchange_operator_proxy.hpp
    compiler/physical_query_plan/operator_proxy/export_operator_proxy.cpp
    compiler/physical_query_plan/operator_proxy/export_operator_proxy.hpp
    compiler/physical_query_plan/operator_proxy/filter_operator_proxy.cpp
    compiler/physical_query_plan/operator_proxy/filter_operator_proxy.hpp
    compiler/physical_query_plan/operator_proxy/import_operator_proxy.cpp
    compiler/physical_query_plan/operator_proxy/import_operator_proxy.hpp
    compiler/physical_query_plan/operator_proxy/import_options.cpp
    compiler/physical_query_plan/operator_proxy/import_options.hpp
    compiler/physical_query_plan/operator_proxy/join_operator_proxy.cpp
    compiler/physical_query_plan/operator_proxy/join_operator_proxy.hpp
    compiler/physical_query_plan/operator_proxy/limit_operator_proxy.cpp
    compiler/physical_query_plan/operator_proxy/limit_operator_proxy.hpp
    compiler/physical_query_plan/operator_proxy/partition_operator_proxy.cpp
    compiler/physical_query_plan/operator_proxy/partition_operator_proxy.hpp
    compiler/physical_query_plan/operator_proxy/projection_operator_proxy.cpp
    compiler/physical_query_plan/operator_proxy/projection_operator_proxy.hpp
    compiler/physical_query_plan/operator_proxy/sort_operator_proxy.cpp
    compiler/physical_query_plan/operator_proxy/sort_operator_proxy.hpp
    compiler/physical_query_plan/operator_proxy/union_operator_proxy.cpp
    compiler/physical_query_plan/operator_proxy/union_operator_proxy.hpp
    compiler/physical_query_plan/pipeline_fragment_template.cpp
    compiler/physical_query_plan/pipeline_fragment_template.hpp
    compiler/physical_query_plan/pqp_pipeline.cpp
    compiler/physical_query_plan/pqp_pipeline.hpp
    compiler/physical_query_plan/pqp_serialization.cpp
    compiler/physical_query_plan/pqp_serialization.hpp
    compiler/physical_query_plan/pqp_utils.cpp
    compiler/physical_query_plan/pqp_utils.hpp
    compiler/plan_utils.hpp
    compiler/pqp_generator/etl_pqp_generator.cpp
    compiler/pqp_generator/etl_pqp_generator.hpp
    compiler/pqp_generator/tpch_pqp_generator.cpp
    compiler/pqp_generator/tpch_pqp_generator.hpp
)

set(
    SHARED_EXPRESSION_SOURCES

    expression/abstract_expression.cpp
    expression/abstract_expression.hpp
    expression/abstract_predicate_expression.cpp
    expression/abstract_predicate_expression.hpp
    expression/aggregate_expression.cpp
    expression/aggregate_expression.hpp
    expression/arithmetic_expression.cpp
    expression/arithmetic_expression.hpp
    expression/between_expression.cpp
    expression/between_expression.hpp
    expression/binary_predicate_expression.cpp
    expression/binary_predicate_expression.hpp
    expression/case_expression.cpp
    expression/case_expression.hpp
    expression/cast_expression.cpp
    expression/cast_expression.hpp
    expression/evaluation/expression_evaluator.cpp
    expression/evaluation/expression_evaluator.hpp
    expression/evaluation/expression_functors.hpp
    expression/evaluation/expression_result.hpp
    expression/evaluation/expression_result_views.hpp
    expression/expression_functional.cpp
    expression/expression_functional.hpp
    expression/expression_precedence.hpp
    expression/expression_serialization.cpp
    expression/expression_serialization.hpp
    expression/expression_utils.cpp
    expression/expression_utils.hpp
    expression/extract_expression.cpp
    expression/extract_expression.hpp
    expression/in_expression.cpp
    expression/in_expression.hpp
    expression/is_null_expression.cpp
    expression/is_null_expression.hpp
    expression/list_expression.cpp
    expression/list_expression.hpp
    expression/logical_expression.cpp
    expression/logical_expression.hpp
    expression/pqp_column_expression.cpp
    expression/pqp_column_expression.hpp
    expression/string_agg_aggregate_expression.cpp
    expression/string_agg_aggregate_expression.hpp
    expression/unary_minus_expression.cpp
    expression/unary_minus_expression.hpp
    expression/value_expression.cpp
    expression/value_expression.hpp
)

set(
    SHARED_METADATA_SOURCES

    metadata/schema/abstract_catalog.hpp
    metadata/schema/table_partition.cpp
    metadata/schema/table_partition.hpp
    metadata/schema/table_schema.cpp
    metadata/schema/table_schema.hpp
)

set(
    SHARED_METERING_SOURCES

    metering/request_tracker/request_monitor.cpp
    metering/request_tracker/request_monitor.hpp
    metering/request_tracker/request_tracker.cpp
    metering/request_tracker/request_tracker.hpp
)

set(
    SHARED_OPERATOR_SOURCES

    operator/abstract_aggregate_operator.cpp
    operator/abstract_aggregate_operator.hpp
    operator/aggregate_hash_operator.cpp
    operator/aggregate_hash_operator.hpp
    operator/abstract_operator.cpp
    operator/abstract_operator.hpp
    operator/alias_operator.cpp
    operator/alias_operator.hpp
    operator/execution_context.cpp
    operator/execution_context.hpp
    operator/export_operator.cpp
    operator/export_operator.hpp
    operator/filter_operator.cpp
    operator/filter_operator.hpp
    operator/filter/abstract_filter_implementation.hpp
    operator/filter/expression_evaluator_filter_implementation.cpp
    operator/filter/expression_evaluator_filter_implementation.hpp
    operator/hash_join_operator.cpp
    operator/hash_join_operator.hpp
    operator/import_operator.cpp
    operator/import_operator.hpp
    operator/join_operator_predicate.hpp
    operator/partition_operator.cpp
    operator/partition_operator.hpp
    operator/partitioning_function.cpp
    operator/partitioning_function.hpp
    operator/projection_operator.cpp
    operator/projection_operator.hpp
    operator/sort_operator.cpp
    operator/sort_operator.hpp
    operator/table_wrapper.hpp
)

set(
    SHARED_SCHEDULER_SOURCES

    scheduler/coordinator/lambda_executor.cpp
    scheduler/coordinator/lambda_executor.hpp
    scheduler/coordinator/pqp_pipeline_scheduler.cpp
    scheduler/coordinator/pqp_pipeline_scheduler.hpp
    scheduler/warmup_strategy.cpp
    scheduler/warmup_strategy.hpp
    scheduler/worker/abstract_task.cpp
    scheduler/worker/abstract_task.hpp
    scheduler/worker/task_executor.cpp
    scheduler/worker/task_executor.hpp
    scheduler/worker/generic_task.cpp
    scheduler/worker/generic_task.hpp
    scheduler/worker/operator_task.cpp
    scheduler/worker/operator_task.hpp
    scheduler/worker/fragment_scheduler.cpp
    scheduler/worker/fragment_scheduler.hpp
)

set(
    SHARED_SERIALIZATION_SOURCES

    serialization/binary_serialization_stream.cpp
    serialization/binary_serialization_stream.hpp
    serialization/schema_serialization.cpp
    serialization/schema_serialization.hpp
)

set(
    SHARED_STORAGE_SOURCES

    storage/backend/abstract_storage.cpp
    storage/backend/abstract_storage.hpp
    storage/backend/dynamodb_storage.cpp
    storage/backend/dynamodb_storage.hpp
    storage/backend/dynamodb_utils.cpp
    storage/backend/dynamodb_utils.hpp
    storage/backend/errors.cpp
    storage/backend/errors.hpp
    storage/backend/filesystem_storage.cpp
    storage/backend/filesystem_storage.hpp
    storage/backend/s3_storage.cpp
    storage/backend/s3_storage.hpp
    storage/backend/s3_utils.cpp
    storage/backend/s3_utils.hpp
    storage/backend/stream.cpp
    storage/backend/stream.hpp
    storage/formats/abstract_chunk_reader.cpp
    storage/formats/abstract_chunk_reader.hpp
    storage/formats/abstract_chunk_writer.cpp
    storage/formats/abstract_chunk_writer.hpp
    storage/formats/csv_reader.cpp
    storage/formats/csv_reader.hpp
    storage/formats/csv_writer.cpp
    storage/formats/csv_writer.hpp
    storage/formats/orc_reader.cpp
    storage/formats/orc_reader.hpp
    storage/formats/orc_writer.cpp
    storage/formats/orc_writer.hpp
    storage/formats/parquet_expression.cpp
    storage/formats/parquet_expression.hpp
    storage/formats/parquet_metadata_reader.cpp
    storage/formats/parquet_metadata_reader.hpp
    storage/formats/parquet_reader.cpp
    storage/formats/parquet_reader.hpp
    storage/formats/parquet_writer.cpp
    storage/formats/parquet_writer.hpp
    storage/io_handle/byte_buffer.cpp
    storage/io_handle/byte_buffer.hpp
    storage/io_handle/input_handler.cpp
    storage/io_handle/input_handler.cpp
    storage/io_handle/object_buffer.cpp
    storage/io_handle/object_buffer.hpp
    storage/storage_types.hpp
    storage/table/abstract_segment.hpp
    storage/table/abstract_table_constraint.cpp
    storage/table/abstract_table_constraint.hpp
    storage/table/base_value_segment.hpp
    storage/table/chunk.cpp
    storage/table/chunk.hpp
    storage/table/chunk_writer.cpp
    storage/table/chunk_writer.hpp
    storage/table/table.cpp
    storage/table/table.hpp
    storage/table/table_column_definition.cpp
    storage/table/table_column_definition.hpp
    storage/table/table_key_constraint.cpp
    storage/table/table_key_constraint.hpp
    storage/table/value_segment.cpp
    storage/table/value_segment.hpp
)

set(
    SHARED_UTILS_SOURCES

    function/function_config.hpp
    utils/array.hpp
    utils/assert.hpp
    utils/compression.cpp
    utils/compression.hpp
    utils/concurrent/concurrent_queue.hpp
    utils/filesystem.cpp
    utils/filesystem.hpp
    utils/git_metadata.cpp
    utils/git_metadata.hpp
    utils/invalid_input_exception.hpp
    utils/json.hpp
    utils/literal.hpp
    utils/map.hpp
    utils/print_directed_acyclic_graph.hpp
    utils/profiling/function_host_information.cpp
    utils/profiling/function_host_information.hpp
    utils/random.hpp
    utils/region.cpp
    utils/region.hpp
    utils/self_name.cpp
    utils/self_name.hpp
    utils/signal_handler.cpp
    utils/signal_handler.hpp
    utils/string.cpp
    utils/string.hpp
    utils/time.cpp
    utils/time.hpp
    utils/unit_conversion.hpp
    utils/vector.hpp
)

set(
    SHARED_SOURCES

    aggregate_traits.hpp
    all_type_variant.cpp
    all_type_variant.hpp
    client/base_client.cpp
    client/base_client.hpp
    constant_mappings.cpp
    constant_mappings.hpp
    constants.hpp
    function/function.cpp
    function/function.hpp
    monitoring/monitoring_types.hpp
    types.cpp
    types.hpp
    ${SHARED_COMPILER_SOURCES}
    ${SHARED_EXPRESSION_SOURCES}
    ${SHARED_METADATA_SOURCES}
    ${SHARED_METERING_SOURCES}
    ${SHARED_OPERATOR_SOURCES}
    ${SHARED_SCHEDULER_SOURCES}
    ${SHARED_SERIALIZATION_SOURCES}
    ${SHARED_STORAGE_SOURCES}
    ${SHARED_UTILS_SOURCES}
)

set(
    SHARED_LIBRARIES

    # Libraries that are listed here will be linked to many different executables. Be careful what you place here,
    # since it will affect the size, and thus, the invocation latency of AWS Lambda functions.
    aws-cpp-sdk-core
    aws-cpp-sdk-dynamodb
    aws-cpp-sdk-ec2
    aws-cpp-sdk-elasticfilesystem
    aws-cpp-sdk-lambda
    aws-cpp-sdk-s3
    aws-cpp-sdk-sqs
    aws-cpp-sdk-xray
    aws-lambda-runtime

    # Link Arrow and Parquet libraries in order of dependencies
    arrow_dataset_static
    parquet_static
    arrow_static
)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(
        STACKTRACE_LIBRARIES

        boost_stacktrace_backtrace
        dl
        backtrace
    )
else()
    set(
        STACKTRACE_LIBRARIES

        boost_stacktrace_noop
    )
endif()

add_library(skyriseSharedLibrary STATIC ${SHARED_SOURCES})

add_dependencies(skyriseSharedLibrary check_git)

target_link_libraries(skyriseSharedLibrary PUBLIC ${SHARED_LIBRARIES} ${STACKTRACE_LIBRARIES})

add_library(skyriseShared INTERFACE)

target_link_libraries(skyriseShared INTERFACE skyriseSharedLibrary)



set(
    COORDINATOR_SOURCES

    client/coordinator_client.cpp
    client/coordinator_client.hpp
    configuration.hpp
    function/function_utils.cpp
    function/function_utils.hpp
    monitoring/lambda_segments_analyzer.cpp
    monitoring/lambda_segments_analyzer.hpp
    tool/function_upload_tool.cpp
    tool/function_upload_tool.hpp
    tool/tool_config.cpp
    tool/tool_config.hpp
    utils/costs/cost_calculator.cpp
    utils/costs/cost_calculator.hpp
    utils/costs/pricing.cpp
    utils/costs/pricing.hpp
    ${COORDINATOR_COMPILER_SOURCES}
    ${COORDINATOR_METADATA_SOURCES}
)

set(
    COORDINATOR_LIBRARIES

    aws-cpp-sdk-glue
    aws-cpp-sdk-iam
    aws-cpp-sdk-logs
    aws-cpp-sdk-monitoring
    aws-cpp-sdk-pricing
    aws-cpp-sdk-ssm

    skyriseShared
)

add_library(skyriseCoordinator STATIC ${COORDINATOR_SOURCES})

target_link_libraries(skyriseCoordinator PUBLIC ${COORDINATOR_LIBRARIES})

set(
    WORKER_SOURCES

    monitoring/metrics_collector.cpp
    monitoring/metrics_collector.hpp
    monitoring/monitoring_handler.cpp
    monitoring/monitoring_handler.hpp
    monitoring/tracer.cpp
    monitoring/tracer.hpp
)

set(
    WORKER_LIBRARIES

    aws-lambda-runtime

    skyriseShared
)

add_library(skyriseWorker STATIC ${WORKER_SOURCES})

target_link_libraries(skyriseWorker PUBLIC ${WORKER_LIBRARIES})
