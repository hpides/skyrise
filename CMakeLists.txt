cmake_minimum_required(VERSION 3.10 FATAL_ERROR)

project(skyrise
    VERSION 1.0.0
    LANGUAGES C CXX)

if(POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW)
endif()

option(SKYRISE_BUILD_GRAVITON2 "Enable Graviton2 specific processor features" ON)
option(SKYRISE_ENABLE_CCACHE "Enable build caching with Ccache" ON)
option(SKYRISE_ENABLE_CLANG_FORMAT "Enable formatting with Clang-Format" ON)
option(SKYRISE_ENABLE_CLANG_TIDY "Enable static analysis with Clang-Tidy" OFF)
option(SKYRISE_ENABLE_CPPCHECK "Enable static analysis with Cppcheck" OFF)
option(SKYRISE_ENABLE_CPPLINT "Enable static analysis with Cpplint" OFF)
option(SKYRISE_ENABLE_GOOGLE_ADDRESS_SANITIZER "Enable memory corruption detection with ASan" OFF)
option(SKYRISE_ENABLE_GOOGLE_LEAK_SANITIZER "Enable memory leak detection with LSan" OFF)
option(SKYRISE_ENABLE_GOOGLE_THREAD_SANITIZER "Enable data race detection with TSan" OFF)
option(SKYRISE_ENABLE_GOOGLE_UNDEFINED_BEHAVIOR_SANITIZER, "Enable undefined behavior detection with UBSan" OFF)
option(SKYRISE_ENABLE_LLVM_COV "Enable code coverage with llvm-cov" OFF)
option(SKYRISE_ENABLE_SIGNAL_HANDLER_TESTS "Enable execution of signal handler tests" OFF)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_PACKAGE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/pkg)
file(MAKE_DIRECTORY ${CMAKE_PACKAGE_OUTPUT_DIRECTORY})

# Define job pool of size 1 for jobs that require serial execution
set_property(GLOBAL PROPERTY JOB_POOLS serial_jobs=1)

# Set compiler settings for third-party and Skyrise sources
include(cmake/CompilerSettings.cmake)

# Locate third-party packages
set(aws-lambda-runtime_DIR cmake)
find_package(aws-lambda-runtime CONFIG REQUIRED)
find_package(AWSSDK REQUIRED COMPONENTS dynamodb ec2 elasticfilesystem events glue iam lambda logs monitoring pricing s3 sqs
    ssm xray)
find_package(Boost)
# Arrow and Parquet are tightly coupled: Parquet depends on Arrow and ArrowDataSet depends on Parquet
find_package(Arrow REQUIRED)
get_filename_component(ARROW_DIR ${Arrow_CONFIG} DIRECTORY)
find_package(Parquet REQUIRED HINTS ${ARROW_DIR})
find_package(ArrowDataset REQUIRED HINTS ${ARROW_DIR})

# Add third-party sources
add_subdirectory(third_party EXCLUDE_FROM_ALL)

# Set formatter as well as static and dynamic analyzers just for Skyrise sources
include(cmake/Formatter.cmake)
include(cmake/StaticAnalyzers.cmake)
include(cmake/DynamicAnalyzers.cmake)

# Sources
add_subdirectory(src)
