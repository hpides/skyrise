add_compile_options(-Wno-unused-parameter)

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
