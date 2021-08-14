set(CMAKE_CXX_STANDARD_REQUIRED ON)
# The AWS Lambda execution environment contains GCC 7.3, which supports most of C++17 (cf. https://en.cppreference.com/
# w/cpp/compiler_support)
set(CMAKE_CXX_STANDARD 17)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_definitions(SKYRISE_DEBUG=1)
else()
    add_compile_definitions(SKYRISE_DEBUG=0)
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
    ${PROJECT_SOURCE_DIR}/third_party/boost/libs/assert/include
    ${PROJECT_SOURCE_DIR}/third_party/boost/libs/bimap/include
    ${PROJECT_SOURCE_DIR}/third_party/boost/libs/bind/include
    ${PROJECT_SOURCE_DIR}/third_party/boost/libs/concept_check/include
    ${PROJECT_SOURCE_DIR}/third_party/boost/libs/config/include
    ${PROJECT_SOURCE_DIR}/third_party/boost/libs/container_hash/include
    ${PROJECT_SOURCE_DIR}/third_party/boost/libs/core/include
    ${PROJECT_SOURCE_DIR}/third_party/boost/libs/detail/include
    ${PROJECT_SOURCE_DIR}/third_party/boost/libs/foreach/include
    ${PROJECT_SOURCE_DIR}/third_party/boost/libs/integer/include
    ${PROJECT_SOURCE_DIR}/third_party/boost/libs/iterator/include
    ${PROJECT_SOURCE_DIR}/third_party/boost/libs/move/include
    ${PROJECT_SOURCE_DIR}/third_party/boost/libs/multi_index/include
    ${PROJECT_SOURCE_DIR}/third_party/boost/libs/mpl/include
    ${PROJECT_SOURCE_DIR}/third_party/boost/libs/preprocessor/include
    ${PROJECT_SOURCE_DIR}/third_party/boost/libs/serialization/include
    ${PROJECT_SOURCE_DIR}/third_party/boost/libs/static_assert/include
    ${PROJECT_SOURCE_DIR}/third_party/boost/libs/throw_exception/include
    ${PROJECT_SOURCE_DIR}/third_party/boost/libs/tuple/include
    ${PROJECT_SOURCE_DIR}/third_party/boost/libs/type_traits/include
    ${PROJECT_SOURCE_DIR}/third_party/boost/libs/utility/include
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
