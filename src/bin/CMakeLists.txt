add_executable(
    skyrise

    skyrise.cpp
)

target_link_libraries(
    skyrise

    PRIVATE
    skyriseCoordinator
)

add_subdirectory(function)
