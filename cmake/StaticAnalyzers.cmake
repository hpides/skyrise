option(SKYRISE_ENABLE_CLANG_TIDY "Enable static analysis with clang-tidy" OFF)
option(SKYRISE_ENABLE_CPPCHECK "Enable static analysis with cppcheck" OFF)
option(SKYRISE_ENABLE_CPPLINT "Enable static analysis with cpplint" OFF)

# Clang-Tidy
if(SKYRISE_ENABLE_CLANG_TIDY)
    find_program(CLANG_TIDY clang-tidy)
    if(CLANG_TIDY)
        # Configuration in .clang-tidy file
        set(CMAKE_CXX_CLANG_TIDY ${CLANG_TIDY})
        message(STATUS "Clang-Tidy enabled")
    else()
        message(FATAL_ERROR "Clang-Tidy requested but executable not found")
    endif()
endif()

# Cppcheck
if(SKYRISE_ENABLE_CPPCHECK)
    find_program(CPPCHECK cppcheck)
    if(CPPCHECK)
        set(CMAKE_CXX_CPPCHECK ${CPPCHECK}
            --enable=all
            --inconclusive
            --std=c++17
            --suppress=missingInclude
        )
        message(STATUS "Cppcheck enabled")
    else()
        message(FATAL_ERROR "Cppcheck requested but executable not found")
    endif()
endif()

# Cpplint
if(SKYRISE_ENABLE_CPPLINT)
    find_program(CPPLINT cpplint.py)
    if(CPPLINT)
        set(CMAKE_CXX_CPPLINT ${CPPLINT}
            --counting=detailed
            --extensions=cpp,hpp
            --linelength=120
            --verbose=0
        )
        message(STATUS "Cpplint enabled")
    else()
        message(FATAL_ERROR "Cpplint requested but executable not found")
    endif()
endif()
