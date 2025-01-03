# Clang-Format
if(SKYRISE_ENABLE_CLANG_FORMAT)
    find_program(CLANG_FORMAT clang-format)
    if(CLANG_FORMAT)
        find_package(Python3 REQUIRED)
        # Configuration in .clang-format file
        add_custom_target(
            clang-format-edit ALL

            COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/script/build/run_clang_format.py
            --clang_format_binary clang-format
            --source_dir ${CMAKE_SOURCE_DIR}/src
            --fix
            --quiet
        )
        message(STATUS "Clang-Format enabled")
    else()
        message(FATAL_ERROR "Clang-Format requested but executable not found")
    endif()
endif()
