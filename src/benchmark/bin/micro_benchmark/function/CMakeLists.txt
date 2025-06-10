add_executable(
    skyriseMinimalFunction

    minimal_function.cpp
)

target_link_libraries(
    skyriseMinimalFunction

    PRIVATE
    aws-lambda-runtime
)

aws_lambda_package_target(skyriseMinimalFunction)

add_custom_target(
    sized-functions ALL
    COMMAND ${CMAKE_SOURCE_DIR}/script/benchmark/build/sized_function_packager.sh ${CMAKE_PACKAGE_OUTPUT_DIRECTORY}
    DEPENDS aws-lambda-package-skyriseMinimalFunction
)

add_executable(
    skyriseNetworkIoFunction

    network_io_function.cpp
    network_io_function.hpp
)

target_link_libraries(
    skyriseNetworkIoFunction

    PRIVATE
    skyriseBenchmarkUtils
    skyriseWorker
)

aws_lambda_package_target(skyriseNetworkIoFunction)

add_executable(
    skyriseSimpleFunction

    simple_function.cpp
    simple_function.hpp
)

target_link_libraries(
    skyriseSimpleFunction

    PRIVATE
    skyriseWorker
)

aws_lambda_package_target(skyriseSimpleFunction)

add_executable(
    skyriseStorageIoFunction

    storage_io_function.cpp
    storage_io_function.hpp
)

target_link_libraries(
    skyriseStorageIoFunction

    PRIVATE
    skyriseBenchmarkUtils
    skyriseWorker
)

aws_lambda_package_target(skyriseStorageIoFunction)
