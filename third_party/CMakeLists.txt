set(CMAKE_INSTALL_PREFIX ${CMAKE_LIBRARY_OUTPUT_DIRECTORY})

# benchmark
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "Enable testing of the benchmark library.")
add_subdirectory(benchmark)

# googletest
add_compile_options(-Wno-sign-conversion)
add_subdirectory(googletest)

include(FetchContent)

# CLI11
FetchContent_Declare(
    CLI11
    GIT_REPOSITORY https://github.com/CLIUtils/CLI11.git
    GIT_TAG v2.4.1
)
FetchContent_MakeAvailable(CLI11)
