include(CMakeFindDependencyMacro)

find_dependency(CURL)

set(AWS_LAMBDA_PACKAGING_SCRIPT ${PROJECT_SOURCE_DIR}/third_party/aws-lambda-cpp/packaging/packager)
set(SKYRISE_STATIC_LAMBDA_PACKAGING_SCRIPT ${PROJECT_SOURCE_DIR}/script/static_function_packager.sh)

function(aws_lambda_package_target target)
    find_program(LSB_RELEASE_EXEC lsb_release)
    execute_process(COMMAND ${LSB_RELEASE_EXEC} -is
        OUTPUT_VARIABLE LSB_RELEASE_ID_SHORT
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )

    # The ARM build does currently not support static linking.
    if (${LSB_RELEASE_ID_SHORT} STREQUAL "Amazon" AND NOT ${CMAKE_HOST_SYSTEM_PROCESSOR} STREQUAL "aarch64")

        set(RUN_STRIP)
        if (CMAKE_BUILD_TYPE STREQUAL "Release" OR CMAKE_BUILD_TYPE STREQUAL "MinSizeRel")
            set(RUN_STRIP "-s")
        endif()

        add_custom_target(
            aws-lambda-package-${target} ALL
            COMMAND ${SKYRISE_STATIC_LAMBDA_PACKAGING_SCRIPT} ${RUN_STRIP} $<TARGET_FILE:${target}>
            DEPENDS ${target}
            WORKING_DIRECTORY ${CMAKE_PACKAGE_OUTPUT_DIRECTORY}
            JOB_POOL serial_jobs
        )

        unset(RUN_STRIP)
    
    else()

        add_custom_target(
            aws-lambda-package-${target} ALL
            COMMAND ${AWS_LAMBDA_PACKAGING_SCRIPT} ${PACKAGER_NO_LIBC} $<TARGET_FILE:${target}>
            DEPENDS ${target}
            WORKING_DIRECTORY ${CMAKE_PACKAGE_OUTPUT_DIRECTORY}
            JOB_POOL serial_jobs
        )

    endif()
endfunction()
