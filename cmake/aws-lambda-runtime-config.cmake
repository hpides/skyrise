include(CMakeFindDependencyMacro)

find_dependency(CURL)

set(AWS_LAMBDA_PACKAGING_SCRIPT ${PROJECT_SOURCE_DIR}/third_party/aws-lambda-cpp/packaging/packager)
set(STRIP_FUNCTION_PACKAGE_SCRIPT ${PROJECT_SOURCE_DIR}/script/strip_function_package.sh)

function(aws_lambda_package_target target)
    find_program(LSB_RELEASE_EXEC lsb_release)
    execute_process(COMMAND ${LSB_RELEASE_EXEC} -is
        OUTPUT_VARIABLE LSB_RELEASE_ID_SHORT
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )

    if (${LSB_RELEASE_ID_SHORT} STREQUAL "Amazon")
        set (PACKAGER_NO_LIBC "-d")

        add_custom_target(
            aws-lambda-package-${target}-stripped ALL
            COMMAND ${STRIP_FUNCTION_PACKAGE_SCRIPT} ${target}.zip
            DEPENDS aws-lambda-package-${target}
            WORKING_DIRECTORY ${CMAKE_PACKAGE_OUTPUT_DIRECTORY}
            JOB_POOL serial_jobs
        )
    endif()

    add_custom_target(
        aws-lambda-package-${target} ALL
        COMMAND ${AWS_LAMBDA_PACKAGING_SCRIPT} ${PACKAGER_NO_LIBC} $<TARGET_FILE:${target}>
        DEPENDS ${target}
        WORKING_DIRECTORY ${CMAKE_PACKAGE_OUTPUT_DIRECTORY}
        JOB_POOL serial_jobs
    )
endfunction()
