set(CMAKE_CXX_STANDARD_REQUIRED ON)
# The AWS Lambda execution environment contains GCC 7.2, which supports most of C++17 (cf. https://en.cppreference.com/
# w/cpp/compiler_support)
set(CMAKE_CXX_STANDARD 17)

include_directories(
    ${PROJECT_SOURCE_DIR}/third_party/aws-sdk-cpp/aws-cpp-sdk-core/include
    ${PROJECT_SOURCE_DIR}/third_party/aws-sdk-cpp/aws-cpp-sdk-s3/include
)

add_compile_options(-pthread -Wall -Wextra -Wpedantic -Werror)

add_subdirectory(benchmark)
#add_subdirectory(bin)
#add_subdirectory(lib)
#add_subdirectory(test)
