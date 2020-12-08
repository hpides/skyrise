add_compile_options(
    -Wno-array-bounds
    -Wno-extra-semi-stmt
    -Wno-suggest-destructor-override
    -Wno-suggest-override
    -Wno-unused-parameter
)

set(CMAKE_INSTALL_PREFIX ${CMAKE_LIBRARY_OUTPUT_DIRECTORY})

# aws-lambda-cpp
set(BUILD_SHARED_LIBS OFF CACHE BOOL "Enable shared libraries for AWS Lambda runtime for C++")
set(ENABLE_LTO ON CACHE BOOL "Enable LTO for AWS Lambda runtmie for C++")
set(ENABLE_TESTS OFF CACHE BOOL "Enable tests for AWS Lambda runtime for C++")
add_subdirectory(aws-lambda-cpp)

# aws-sdk-cpp
set(BUILD_ONLY iam;lambda;monitoring;pricing;s3;sqs;xray)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "Enable shared libraries for AWS SDK for C++")
set(CPP_STANDARD 17)
set(CUSTOM_MEMORY_MANAGEMENT OFF)
set(ENABLE_TESTING OFF CACHE BOOL "Enable tests for AWS SDK for C++")
set(MINIMIZE_SIZE ON CACHE BOOL "Enable unity build and other binary size reduction measures for AWS SDK for C++")
set(TARGET_ARCH LINUX)
add_subdirectory(aws-sdk-cpp)

# googletest
add_subdirectory(googletest)

# orc
set(BUILD_JAVA OFF CACHE BOOL "Include ORC Java library in the build process")
set(BUILD_LIBHDFSPP OFF CACHE BOOL "Include LIBHDFSPP library in the build process")
set(BUILD_CPP_TESTS OFF CACHE BOOL "Build the googletest unit tests")
set(BUILD_TOOLS OFF CACHE BOOL "Build the tools")
add_subdirectory(orc)

find_package(Threads REQUIRED)
set(ORC_THIRDPARTY_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/c++/libs/thirdparty)
add_library(orcWithDependencies INTERFACE)
target_link_libraries(
    orcWithDependencies

    INTERFACE
    orc
    Threads::Threads
    ${ORC_THIRDPARTY_LIBRARY_OUTPUT_DIRECTORY}/lz4_ep-install/lib/liblz4.a
    ${ORC_THIRDPARTY_LIBRARY_OUTPUT_DIRECTORY}/protobuf_ep-install/lib/libprotobuf.a
    ${ORC_THIRDPARTY_LIBRARY_OUTPUT_DIRECTORY}/snappy_ep-install/lib/libsnappy.a
    ${ORC_THIRDPARTY_LIBRARY_OUTPUT_DIRECTORY}/zlib_ep-install/lib/libz.a
    ${ORC_THIRDPARTY_LIBRARY_OUTPUT_DIRECTORY}/zstd_ep-install/lib/libzstd.a
)

# tpch-dbgen
add_subdirectory(tpch-dbgen)
