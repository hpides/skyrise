set(CMAKE_CXX_STANDARD_REQUIRED ON)
# The AWS Lambda execution environment contains GCC 7.3, which supports most of C++17 (cf. https://en.cppreference.com/
# w/cpp/compiler_support)
set(CMAKE_CXX_STANDARD 17)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_definitions(SKYRISE_DEBUG=1)
else()
    add_compile_definitions(SKYRISE_DEBUG=0)
endif()

list(APPEND compile_options -pthread -Wall -Wextra -Wpedantic -Werror)

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
    ${PROJECT_SOURCE_DIR}/third_party/orc/c++/include
    ${PROJECT_SOURCE_DIR}/third_party/termcolor/include
    ${PROJECT_SOURCE_DIR}/third_party/tpch-dbgen
    ${CMAKE_BINARY_DIR}/third_party/orc/c++/include
)

add_subdirectory(benchmark)
add_subdirectory(bin)
add_subdirectory(lib)
add_subdirectory(test)
