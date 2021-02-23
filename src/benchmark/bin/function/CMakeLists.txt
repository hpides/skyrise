add_executable(
    skyriseFunctionHostId

    function_host_id.cpp
    function_host_id.hpp
)

target_link_libraries(
    skyriseFunctionHostId

    PRIVATE
    skyriseWorker
)

aws_lambda_package_target(skyriseFunctionHostId)

add_executable(
    skyriseFunctionMinimal

    function_minimal.cpp
)

target_link_libraries(
    skyriseFunctionMinimal

    PRIVATE
    aws-lambda-runtime
)

aws_lambda_package_target(skyriseFunctionMinimal)

add_custom_target(
    sized-functions ALL
    COMMAND ${CMAKE_SOURCE_DIR}/script/benchmark/sized_function_packager.sh ${CMAKE_PACKAGE_OUTPUT_DIRECTORY}
    DEPENDS aws-lambda-package-skyriseFunctionMinimal
)

add_executable(
    skyriseFunctionReadS3

    function_read_s3.cpp
    function_read_s3.hpp
)

target_link_libraries(
    skyriseFunctionReadS3

    PRIVATE
    skyriseWorker
)

aws_lambda_package_target(skyriseFunctionReadS3)

add_executable(
    skyriseFunctionSimple

    function_simple.cpp
    function_simple.hpp
)

target_link_libraries(
    skyriseFunctionSimple

    PRIVATE
    skyriseWorker
)

aws_lambda_package_target(skyriseFunctionSimple)

add_executable(
    skyriseFunctionWriteS3

    function_write_s3.cpp
    function_write_s3.hpp
)

target_link_libraries(
    skyriseFunctionWriteS3

    PRIVATE
    skyriseWorker
)

aws_lambda_package_target(skyriseFunctionWriteS3)
