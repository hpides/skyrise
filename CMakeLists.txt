cmake_minimum_required(VERSION 3.9 FATAL_ERROR)

project(skyrise
    VERSION 0.1.0
    LANGUAGES C CXX)

option(SKYRISE_ENABLE_CCACHE "Enable build caching with Ccache" ON)
option(SKYRISE_ENABLE_CLANG_FORMAT "Enable formatting with clang-format" OFF)
option(SKYRISE_ENABLE_CLANG_TIDY "Enable static analysis with clang-tidy" OFF)
option(SKYRISE_ENABLE_CPPCHECK "Enable static analysis with cppcheck" OFF)
option(SKYRISE_ENABLE_CPPLINT "Enable static analysis with cpplint" OFF)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_PACKAGE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/pkg)
file(MAKE_DIRECTORY ${CMAKE_PACKAGE_OUTPUT_DIRECTORY})

# Define job pool of size 1 for jobs that require serial execution
set_property(GLOBAL PROPERTY JOB_POOLS serial_jobs=1)

# Set compiler settings for third-party and Skyrise sources
include(cmake/CompilerSettings.cmake)

# Third-party sources
add_subdirectory(third_party EXCLUDE_FROM_ALL)

set(aws-lambda-runtime_DIR cmake)
find_package(aws-lambda-runtime CONFIG REQUIRED)

# Set formatter and static analyzers just for Skyrise sources
include(cmake/Formatter.cmake)
include(cmake/StaticAnalyzers.cmake)

# Sources
add_subdirectory(src)
