add_compile_options(-Wno-array-bounds -Wno-extra-semi-stmt -Wno-unused-parameter)

set(CMAKE_INSTALL_PREFIX ${CMAKE_LIBRARY_OUTPUT_DIRECTORY})

# aws-lambda-cpp
set(BUILD_SHARED_LIBS ON CACHE BOOL "Enable shared libraries for AWS Lambda runtime for C++")
set(ENABLE_LTO ON CACHE BOOL "Enable LTO for AWS Lambda runtmie for C++")
set(ENABLE_TESTS OFF CACHE BOOL "Enable tests for AWS Lambda runtime for C++")
add_subdirectory(aws-lambda-cpp)

# aws-sdk-cpp
set(BUILD_ONLY ce;elasticache;lambda;monitoring;s3;xray)
set(BUILD_SHARED_LIBS ON CACHE BOOL "Enable shared libraries for AWS SDK for C++")
set(CPP_STANDARD 17)
set(CUSTOM_MEMORY_MANAGEMENT OFF)
set(ENABLE_TESTING OFF CACHE BOOL "Enable tests for AWS SDK for C++")
set(MINIMIZE_SIZE ON CACHE BOOL "Enable unity build and other binary size reduction measures for AWS SDK for C++")
set(TARGET_ARCH LINUX)
add_subdirectory(aws-sdk-cpp)

# orc
set(BUILD_JAVA OFF CACHE BOOL "Include ORC Java library in the build process")
set(BUILD_LIBHDFSPP OFF CACHE BOOL "Include LIBHDFSPP library in the build process")
set(BUILD_CPP_TESTS OFF CACHE BOOL "Build the googletest unit tests")
set(BUILD_TOOLS OFF CACHE BOOL "Build the tools")
add_subdirectory(orc)

set(ORC_THIRDPARTY_LIB_DIR ${CMAKE_BINARY_DIR}/c++/libs/thirdparty)

find_package(Threads REQUIRED)
add_library(orc-with-dependencies INTERFACE)
target_link_libraries(
    orc-with-dependencies

    INTERFACE
    orc
    Threads::Threads
    ${ORC_THIRDPARTY_LIB_DIR}/protobuf_ep-install/lib/libprotobuf.a
    ${ORC_THIRDPARTY_LIB_DIR}/zlib_ep-install/lib/libz.a
    ${ORC_THIRDPARTY_LIB_DIR}/snappy_ep-install/lib/libsnappy.a
    ${ORC_THIRDPARTY_LIB_DIR}/zstd_ep-install/lib/libzstd.a
    ${ORC_THIRDPARTY_LIB_DIR}/lz4_ep-install/lib/liblz4.a
)
