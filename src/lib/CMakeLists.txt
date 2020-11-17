set(PRE_CONFIGURE_FILE "utils/git_metadata.cpp.in")
set(POST_CONFIGURE_FILE "utils/git_metadata.cpp")
include(${PROJECT_SOURCE_DIR}/third_party/cmake-git-version-tracking/git_watcher.cmake)

set(
    SHARED_SOURCES
    utils/array.hpp
    utils/assert.hpp
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
)

set(
    SHARED_LIBRARIES
    aws-cpp-sdk-core
)

add_library(skyriseShared SHARED ${SHARED_SOURCES})

add_dependencies(skyriseShared check_git)

target_link_libraries(skyriseShared PUBLIC ${SHARED_LIBRARIES})

set(
    COORDINATOR_SOURCES
    client/client_aws.cpp
    client/client_aws.hpp
    configuration.hpp
    limits.hpp
    utils/costs/cost_calculator.cpp
    utils/costs/cost_calculator.hpp
    utils/costs/pricing.cpp
    utils/costs/pricing.hpp
#   utils/profiling/function_coldstart.(cpp|hpp)
)

set(
    COORDINATOR_LIBRARIES
    aws-cpp-sdk-iam
    aws-cpp-sdk-lambda
    aws-cpp-sdk-monitoring
    aws-cpp-sdk-pricing
    aws-cpp-sdk-s3
    aws-cpp-sdk-sqs
    aws-cpp-sdk-xray
    skyriseShared
)

add_library(skyriseCoordinator SHARED ${COORDINATOR_SOURCES})

target_link_libraries(skyriseCoordinator PUBLIC ${COORDINATOR_LIBRARIES})

set(
    STORAGE_SOURCES
    storage/backend/abstract_storage.hpp
    storage/backend/storage_filesystem.cpp
    storage/backend/storage_filesystem.hpp
    storage/backend/storage_s3.cpp
    storage/backend/storage_s3.hpp
    storage/formats/abstract_formatter.cpp
    storage/formats/abstract_formatter.hpp
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
    WORKER_SOURCES
    utils/profiling/function_host_information.cpp
    utils/profiling/function_host_information.hpp
    ${SHARED_SOURCES}
    ${STORAGE_SOURCES}
)

set(
    WORKER_LIBRARIES
    aws-cpp-sdk-s3
    aws-lambda-runtime
    skyriseShared
)

add_library(skyriseWorker SHARED ${WORKER_SOURCES})

target_link_libraries(skyriseWorker PUBLIC ${WORKER_LIBRARIES})
