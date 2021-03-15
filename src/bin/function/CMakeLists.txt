add_executable(
    skyriseFunctionStatisticsCollector

    function_statistics_collector.cpp
    function_statistics_collector.hpp
)

target_link_libraries(
    skyriseFunctionStatisticsCollector

    PRIVATE
    skyriseWorker
)

aws_lambda_package_target(skyriseFunctionStatisticsCollector)
