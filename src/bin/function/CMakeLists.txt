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

add_executable(
    skyriseFunctionManifestMerger

    function_manifest_merger.cpp
    function_manifest_merger.hpp
)

target_link_libraries(
    skyriseFunctionManifestMerger

    PRIVATE
    skyriseWorker
)

aws_lambda_package_target(skyriseFunctionManifestMerger)
