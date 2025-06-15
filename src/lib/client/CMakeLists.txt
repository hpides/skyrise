add_library(skyriseClient
    base_client.cpp
    base_client.hpp
    coordinator_client.cpp
    coordinator_client.hpp
)

target_include_directories(skyriseClient
    PUBLIC
    ${PROJECT_SOURCE_DIR}/src/lib
)

target_link_libraries(skyriseClient
    PUBLIC
    aws-cpp-sdk-s3
    aws-cpp-sdk-sqs
    aws-cpp-sdk-core
) 