add_executable(
    skyriseConsole
    
    console.cpp
)

target_link_libraries(
    skyriseConsole

    PRIVATE
    skyriseCoordinator
)

add_subdirectory(function)
