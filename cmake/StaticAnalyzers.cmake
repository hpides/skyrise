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
        set(CPPLINT_FILTERS "-build/c++11,-build/include_order,-build/include_what_you_use,")
        set(CPPLINT_FILTERS "${CPPLINT_FILTERS}-legal/copyright,-readability/todo,-runtime/string,")
        set(CPPLINT_FILTERS "${CPPLINT_FILTERS}-whitespace/braces,-whitespace/newline")
        set(CMAKE_CXX_CPPLINT ${CPPLINT}
            --counting=detailed
            --extensions=cpp,hpp
            --linelength=120
            --filter=${CPPLINT_FILTERS}
            --verbose=0
        )
        message(STATUS "Cpplint enabled")
    else()
        message(FATAL_ERROR "Cpplint requested but executable not found")
    endif()
endif()

# LLVM-Cov
if(SKYRISE_ENABLE_LLVM_COV)
    find_program(LLVM_COV_PATH llvm-cov)
    find_program(LLVM_PROFDATA_PATH llvm-profdata)

    if(NOT LLVM_COV_PATH OR NOT LLVM_PROFDATA_PATH)
        message(STATUS "llvm-cov not found")
    else()
        add_compile_options(-fprofile-instr-generate -fcoverage-mapping)
        add_link_options(-fprofile-instr-generate -fcoverage-mapping)

        message(STATUS "LLVM-Cov enabled")
    endif()
endif()
