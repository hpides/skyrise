include(CMakeFindDependencyMacro)

find_dependency(CURL)

set(AWS_LAMBDA_PACKAGING_SCRIPT ${PROJECT_SOURCE_DIR}/third_party/aws-lambda-cpp/packaging/packager)
function(aws_lambda_package_target target)
    set(options OPTIONAL NO_LIBC)
    cmake_parse_arguments(PACKAGER "${options}" "" "" ${ARGN})
    if (${PACKAGER_NO_LIBC})
        set (PACKAGER_NO_LIBC "-d")
    else()
        set (PACKAGER_NO_LIBC "")
    endif()
    add_custom_target(
        aws-lambda-package-${target} ALL

        COMMAND ${AWS_LAMBDA_PACKAGING_SCRIPT} ${PACKAGER_NO_LIBC} $<TARGET_FILE:${target}>
        DEPENDS ${target}
        WORKING_DIRECTORY ${CMAKE_PACKAGE_OUTPUT_DIRECTORY}
    )
endfunction()