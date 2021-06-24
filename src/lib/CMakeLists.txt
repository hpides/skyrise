set(PRE_CONFIGURE_FILE "utils/git_metadata.cpp.in")
set(POST_CONFIGURE_FILE "utils/git_metadata.cpp")
include(${PROJECT_SOURCE_DIR}/third_party/cmake-git-version-tracking/git_watcher.cmake)

set(
    SERIALIZATION_SOURCES
    serialization/binary_serialization_stream.cpp
    serialization/binary_serialization_stream.hpp
    serialization/schema_serialization.cpp
    serialization/schema_serialization.hpp
)

set(
    STATISTICS_SOURCES
    statistics/manifest_reader.cpp
    statistics/manifest_reader.hpp
    statistics/manifest_writer.cpp
    statistics/manifest_writer.hpp
    statistics/statistics_collector.cpp
    statistics/statistics_collector.hpp
)

set(
    STORAGE_SOURCES
    storage/backend/abstract_storage.hpp
    storage/backend/errors.cpp
    storage/backend/errors.hpp
    storage/backend/storage_filesystem.cpp
    storage/backend/storage_filesystem.hpp
    storage/backend/storage_s3.cpp
    storage/backend/storage_s3.hpp
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
    storage/storage_types.hpp
    storage/table/abstract_segment.hpp
    storage/table/base_value_segment.hpp
    storage/table/chunk.cpp
    storage/table/chunk.hpp
    storage/table/table.cpp
    storage/table/table.hpp
    storage/table/table_column_definition.cpp
    storage/table/table_column_definition.hpp
    storage/table/value_segment.cpp
    storage/table/value_segment.hpp
    storage/table/chunk_writer.cpp
    storage/table/chunk_writer.hpp
)

set(
    STATISTICS_SOURCES
    statistics/manifest_reader.hpp
    statistics/manifest_reader.cpp
    statistics/manifest_writer.hpp
    statistics/manifest_writer.cpp
    statistics/manifest_merger.hpp
    statistics/manifest_merger.cpp
    statistics/statistics_collector.cpp
    statistics/statistics_collector.hpp
)

set(
    SHARED_SOURCES
    all_type_variant.hpp
    expression/abstract_expression.cpp
    expression/abstract_expression.hpp
    expression/abstract_predicate_expression.cpp
    expression/abstract_predicate_expression.hpp
    expression/arithmetic_expression.cpp
    expression/arithmetic_expression.hpp
    expression/between_expression.cpp
    expression/between_expression.hpp
    expression/binary_predicate_expression.cpp
    expression/binary_predicate_expression.hpp
    expression/expression_functional.cpp
    expression/expression_functional.hpp
    expression/expression_precedence.hpp
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
    expression/unary_minus_expression.cpp
    expression/unary_minus_expression.hpp
    expression/value_expression.cpp
    expression/value_expression.hpp
    expression/evaluation/expression_functors.hpp
    expression/evaluation/expression_result.hpp
    expression/evaluation/expression_result_views.hpp
    scheduler/scheduler_types.hpp
    types.hpp
    utils/array.hpp
    utils/assert.hpp
    utils/concurrent/queue.hpp
    utils/filesystem.cpp
    utils/filesystem.hpp
    utils/git_metadata.cpp
    utils/git_metadata.hpp
    utils/invalid_input_exception.hpp
    utils/literal.hpp
    utils/map.hpp
    utils/random.hpp
    utils/string.cpp
    utils/string.hpp
    utils/time.cpp
    utils/time.hpp
    utils/unit_conversion.hpp
    utils/vector.hpp
    ${SERIALIZATION_SOURCES}
    ${STATISTICS_SOURCES}
    ${STORAGE_SOURCES}
)

set(
    SHARED_LIBRARIES
    aws-cpp-sdk-core
    aws-cpp-sdk-s3
    aws-cpp-sdk-sqs
    aws-cpp-sdk-xray
)

add_library(skyriseSharedLibrary STATIC ${SHARED_SOURCES})

add_dependencies(skyriseSharedLibrary check_git)

target_link_libraries(skyriseSharedLibrary PUBLIC ${SHARED_LIBRARIES})

add_library(skyriseShared INTERFACE)
target_link_libraries(skyriseShared
    INTERFACE
    skyriseSharedLibrary
    orcWithDependencies
)

set(
    COORDINATOR_SOURCES
    client/client.cpp
    client/client.hpp
    configuration.hpp
    coordinator.cpp
    coordinator.hpp
    limits.hpp
    monitoring/function_segments.cpp
    monitoring/function_segments.hpp
    scheduler/warm_up_strategy.cpp
    scheduler/warm_up_strategy.hpp
    utils/costs/cost_calculator.cpp
    utils/costs/cost_calculator.hpp
    utils/costs/pricing.cpp
    utils/costs/pricing.hpp
)

set(
    COORDINATOR_LIBRARIES
    aws-cpp-sdk-iam
    aws-cpp-sdk-lambda
    aws-cpp-sdk-monitoring
    aws-cpp-sdk-pricing
    aws-cpp-sdk-xray
    skyriseShared
)

add_library(skyriseCoordinator STATIC ${COORDINATOR_SOURCES})

target_link_libraries(skyriseCoordinator PUBLIC ${COORDINATOR_LIBRARIES})

set(
    WORKER_SOURCES
    cloud_function/function.cpp
    cloud_function/function.hpp
    monitoring/metrics_collector.cpp
    monitoring/metrics_collector.hpp
    monitoring/monitoring_handler.cpp
    monitoring/monitoring_handler.hpp
    monitoring/tracer.cpp
    monitoring/tracer.hpp
    utils/profiling/function_host_information.cpp
    utils/profiling/function_host_information.hpp
)

set(
    WORKER_LIBRARIES
    aws-lambda-runtime
    skyriseShared
)

add_library(skyriseWorker STATIC ${WORKER_SOURCES})

target_link_libraries(skyriseWorker PUBLIC ${WORKER_LIBRARIES})
