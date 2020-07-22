set(
    SHARED_SOURCES
    utils/assert.hpp
    utils/invalid_input_exception.hpp
    utils/string_utils.cpp
    utils/string_utils.hpp
)

set(
    COORDINATOR_SOURCES
    utils/costs/cost_calculator.cpp
    utils/costs/cost_calculator.hpp
    utils/costs/pricing.cpp
    utils/costs/pricing.hpp
#   utils/profiling/function_coldstart.(cpp|hpp)
    ${SHARED_SOURCES}
)

set(
    COORDINATOR_LIBRARIES
    aws-cpp-sdk-core
    aws-cpp-sdk-pricing
)

add_library(skyriseCoordinator SHARED ${COORDINATOR_SOURCES})

target_link_libraries(skyriseCoordinator PUBLIC ${COORDINATOR_LIBRARIES})

set(
    WORKER_SOURCES
    utils/profiling/function_host_information.cpp
    utils/profiling/function_host_information.hpp
    ${SHARED_SOURCES}
)

set(
    WORKER_LIBRARIES
    aws-cpp-sdk-core
)

add_library(skyriseWorker SHARED ${WORKER_SOURCES})

target_link_libraries(skyriseWorker PUBLIC ${WORKER_LIBRARIES})
