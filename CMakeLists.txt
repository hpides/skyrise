cmake_minimum_required(VERSION 3.9 FATAL_ERROR)

project(skyrise
    VERSION 0.1.0
    LANGUAGES CXX)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_PACKAGE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/pkg)
file(MAKE_DIRECTORY ${CMAKE_PACKAGE_OUTPUT_DIRECTORY})

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
