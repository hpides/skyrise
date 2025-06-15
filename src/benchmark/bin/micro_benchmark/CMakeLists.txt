add_executable(directStorageBenchmark direct_storage_benchmark.cpp)

target_include_directories(directStorageBenchmark
    PRIVATE
    ${PROJECT_SOURCE_DIR}/third_party/CLI11/include
)

target_link_libraries(directStorageBenchmark
    PRIVATE
    CLI11::CLI11
    aws-cpp-sdk-s3
    aws-cpp-sdk-core
)
