include(CMakeFindDependencyMacro)

find_dependency(CURL)

set(AWS_LAMBDA_PACKAGING_SCRIPT ${PROJECT_SOURCE_DIR}/third_party/aws-lambda-cpp/packaging/packager)
set(SKYRISE_STATIC_LAMBDA_PACKAGING_SCRIPT ${PROJECT_SOURCE_DIR}/script/build/static_function_packager.sh)

function(aws_lambda_package_target target)
    cmake_host_system_information(RESULT LINUX_DISTRIBUTION QUERY DISTRIB_NAME)

    if (${LINUX_DISTRIBUTION} STREQUAL "Amazon Linux")

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
