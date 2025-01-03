# Check build type
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Debug)
endif()
set(BUILD_TYPES Debug Release RelWithDebInfo MinSizeRel)
if(NOT CMAKE_BUILD_TYPE IN_LIST BUILD_TYPES)
    message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif()

# Check compiler
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 15.0)
        message(FATAL_ERROR "Outdated Clang version: ${CMAKE_CXX_COMPILER_VERSION}")
    endif()
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 11.4)
        message(FATAL_ERROR "Outdated GCC version: ${CMAKE_CXX_COMPILER_VERSION}")
    endif()
else()
    message(FATAL_ERROR "Unsupported compiler: ${CMAKE_CXX_COMPILER_ID}")
endif()

# Check compiler cache
if(SKYRISE_ENABLE_CCACHE)
    find_program(CCACHE ccache)
    if(CCACHE)
        set(CCACHE "CCACHE_DIR=${CMAKE_SOURCE_DIR}/ccache ${CCACHE}")
        set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ${CCACHE})
        message(STATUS "Ccache enabled")
    else()
        message(FATAL_ERROR "Ccache requested but executable not found")
    endif()
endif()

# Activate link time optimization
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)

# Generate compile_commands.json for Clang-based tools
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
