set(
    SHARED_SOURCES
    utils/assert.hpp
    utils/invalid_input_exception.hpp
    utils/literal.hpp
    utils/string.cpp
    utils/string.hpp
    utils/unit_conversion.hpp
)

set(
    SHARED_LIBRARIES
    aws-cpp-sdk-core
)

add_library(skyriseShared SHARED ${SHARED_SOURCES})

target_link_libraries(skyriseShared PUBLIC ${SHARED_LIBRARIES})

set(
    COORDINATOR_SOURCES
    utils/costs/cost_calculator.cpp
    utils/costs/cost_calculator.hpp
    utils/costs/pricing.cpp
    utils/costs/pricing.hpp
#   utils/profiling/function_coldstart.(cpp|hpp)
)

set(
    COORDINATOR_LIBRARIES
    aws-cpp-sdk-core
    aws-cpp-sdk-pricing
    skyriseShared
)

add_library(skyriseCoordinator SHARED ${COORDINATOR_SOURCES})

target_link_libraries(skyriseCoordinator PUBLIC ${COORDINATOR_LIBRARIES})

set(
    WORKER_SOURCES
    utils/profiling/function_host_information.cpp
    utils/profiling/function_host_information.hpp
)

set(
    WORKER_LIBRARIES
    aws-cpp-sdk-core
    aws-lambda-runtime
    skyriseShared
)

add_library(skyriseWorker SHARED ${WORKER_SOURCES})

target_link_libraries(skyriseWorker PUBLIC ${WORKER_LIBRARIES})
