set(CMAKE_CXX_STANDARD_REQUIRED ON)
# The AWS Lambda execution environment contains GCC 7.2, which supports most of C++17 (cf. https://en.cppreference.com/
# w/cpp/compiler_support)
set(CMAKE_CXX_STANDARD 17)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_definitions(SKYRISE_DEBUG)
endif()

add_compile_options(-pthread -Wall -Wextra -Wpedantic -Werror)

include_directories(
    ${PROJECT_SOURCE_DIR}/src/benchmark/lib
    ${PROJECT_SOURCE_DIR}/src/lib
    ${PROJECT_SOURCE_DIR}/third_party/aws-sdk-cpp/aws-cpp-sdk-core/include
    ${PROJECT_SOURCE_DIR}/third_party/aws-sdk-cpp/aws-cpp-sdk-iam/include
    ${PROJECT_SOURCE_DIR}/third_party/aws-sdk-cpp/aws-cpp-sdk-lambda/include
    ${PROJECT_SOURCE_DIR}/third_party/aws-sdk-cpp/aws-cpp-sdk-monitoring/include
    ${PROJECT_SOURCE_DIR}/third_party/aws-sdk-cpp/aws-cpp-sdk-pricing/include
    ${PROJECT_SOURCE_DIR}/third_party/aws-sdk-cpp/aws-cpp-sdk-s3/include
    ${PROJECT_SOURCE_DIR}/third_party/aws-sdk-cpp/aws-cpp-sdk-sqs/include
    ${PROJECT_SOURCE_DIR}/third_party/aws-sdk-cpp/aws-cpp-sdk-xray/include
    ${PROJECT_SOURCE_DIR}/third_party/magic_enum/include
    ${PROJECT_SOURCE_DIR}/third_party/orc/c++/include
    ${CMAKE_BINARY_DIR}/third_party/orc/c++/include
)

add_subdirectory(benchmark)
add_subdirectory(bin)
add_subdirectory(lib)
add_subdirectory(test)
