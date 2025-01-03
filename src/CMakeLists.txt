set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 20)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_definitions(
        SKYRISE_DEBUG=1

        BOOST_STACKTRACE_LINK
        BOOST_STACKTRACE_USE_BACKTRACE
        BOOST_STACKTRACE_BACKTRACE_FORCE_STATIC)
else()
    add_compile_definitions(
        SKYRISE_DEBUG=0

        BOOST_STACKTRACE_USE_NOOP)
endif()

if(SKYRISE_ENABLE_SIGNAL_HANDLER_TESTS STREQUAL "OFF")
    add_compile_definitions(
        SKYRISE_SIGNAL_HANDLER_EXIT=1
    )
else()
    add_compile_definitions(
        SKYRISE_SIGNAL_HANDLER_EXIT=0
    )
endif()

list(APPEND compile_options -pthread -Wall -Wextra -Wpedantic -Werror)
list(APPEND compile_options -Wno-deprecated-enum-float-conversion -Wno-unknown-pragmas)

# Specific flags to exploit Graviton2 processor features (c.f. https://github.com/aws/aws-graviton-getting-started)
if (${CMAKE_HOST_SYSTEM_PROCESSOR} STREQUAL "aarch64" AND SKYRISE_BUILD_GRAVITON2)
    message(STATUS "Graviton2 optimizations enabled")
    list(APPEND compile_options -mcpu=neoverse-n1)
endif()

add_compile_options(${compile_options})

include_directories(
    ${PROJECT_SOURCE_DIR}/src/lib
    ${PROJECT_SOURCE_DIR}/third_party/cxxopts/include
    ${PROJECT_SOURCE_DIR}/third_party/magic_enum/include
    ${PROJECT_SOURCE_DIR}/third_party/termcolor/include
)

add_subdirectory(benchmark)
add_subdirectory(bin)
add_subdirectory(lib)
add_subdirectory(test)
