cmake_minimum_required(VERSION 3.9 FATAL_ERROR)

project(skyrise
    VERSION 0.1.0
    LANGUAGES C CXX)

option(SKYRISE_ENABLE_CCACHE "Enable build caching with Ccache" ON)
option(SKYRISE_ENABLE_CLANG_FORMAT "Enable formatting with Clang-Format" OFF)
option(SKYRISE_ENABLE_CLANG_TIDY "Enable static analysis with Clang-Tidy" OFF)
option(SKYRISE_ENABLE_CPPCHECK "Enable static analysis with Cppcheck" OFF)
option(SKYRISE_ENABLE_CPPLINT "Enable static analysis with Cpplint" OFF)
option(SKYRISE_ENABLE_GOOGLE_ADDRESS_SANITIZER "Enable memory corruption detection with ASan" OFF)
option(SKYRISE_ENABLE_GOOGLE_LEAK_SANITIZER "Enable memory leak detection with LSan" OFF)
option(SKYRISE_ENABLE_LLVM_COV "Enable code coverage with llvm-cov" OFF)

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

# Set formatter as well as static and dynamic analyzers just for Skyrise sources
include(cmake/Formatter.cmake)
include(cmake/StaticAnalyzers.cmake)
include(cmake/DynamicAnalyzers.cmake)

# Sources
add_subdirectory(src)
