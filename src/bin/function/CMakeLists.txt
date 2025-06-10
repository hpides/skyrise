add_executable(
    skyriseCoordinatorFunction

    coordinator_function.cpp
    coordinator_function.hpp
)

target_link_libraries(
    skyriseCoordinatorFunction

    skyriseCoordinator
)

aws_lambda_package_target(skyriseCoordinatorFunction)

add_executable(
    skyriseWorkerFunction

    worker_function.cpp
    worker_function.hpp
)

target_link_libraries(
    skyriseWorkerFunction

    PRIVATE
    skyriseWorker
)

aws_lambda_package_target(skyriseWorkerFunction)
