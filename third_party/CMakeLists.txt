set(CMAKE_INSTALL_PREFIX ${CMAKE_LIBRARY_OUTPUT_DIRECTORY})

# aws-lambda-cpp
add_compile_options(-Wno-unqualified-std-cast-call)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "Enable shared libraries for AWS Lambda runtime for C++")
set(ENABLE_TESTS OFF CACHE BOOL "Enable tests for AWS Lambda runtime for C++")
add_subdirectory(aws-lambda-cpp)

# googletest
add_compile_options(-Wno-sign-conversion)
add_subdirectory(googletest)
