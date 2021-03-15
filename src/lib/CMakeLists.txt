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
    storage/backend/storage_filesystem.cpp
    storage/backend/storage_filesystem.hpp
    storage/backend/storage_s3.cpp
    storage/backend/storage_s3.hpp
    storage/formats/abstract_formatter.cpp
    storage/formats/abstract_formatter.hpp
    storage/formats/csv.cpp
    storage/formats/csv.hpp
    storage/formats/orc.cpp
    storage/formats/orc.hpp
    storage/table/writer.cpp
    storage/table/writer.hpp
    storage/types/abstract_segment.hpp
    storage/types/all_variants.hpp
    storage/types/base_value_segment.hpp
    storage/types/chunk.cpp
    storage/types/chunk.hpp
    storage/types/null_value.hpp
    storage/types/table_column_definition.cpp
    storage/types/table_column_definition.hpp
    storage/types/value_segment.cpp
    storage/types/value_segment.hpp
)

set(
    SHARED_SOURCES
    scheduler/types.hpp
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
