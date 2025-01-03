add_subdirectory(function)

add_executable(
    skyrise

    skyrise.cpp
)

target_link_libraries(
    skyrise

    PRIVATE
    skyriseCoordinator
)
