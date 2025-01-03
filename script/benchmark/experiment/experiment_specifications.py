import itertools

from typing import Callable, Dict, List, Union


class Experiment:

    def __init__(self,
                 identifier: str,
                 executable: str,
                 parameters: List[str],
                 arguments: List[Union[float, int, str]],
                 plots: Dict[str, Callable] = {}):

        self.identifier = identifier
        self.executable = executable
        self.parameters = parameters
        self.arguments = arguments
        self.plots = plots


def init():
    global executables_path
    executables_path = "../../../cmake-build-release/bin"

    global experiments_path
    experiments_path = "../../experiments"

    global experiments
    experiments = []

    ####################################################################################################################
    # skyriseBenchmarkFunctionTemperature
    ####################################################################################################################

    executable = "skyriseBenchmarkFunctionTemperature"
    parameters = [
        "--function_instance_mb_sizes", "--invocation_counts", "--sleep_ms_durations", "--provisioning_factors",
        "--repetition_count"
    ]

    ####################################################################################################################
    # sleep_ms_duration_and_provisioning_factor_to_warm_function_percentage
    ####################################################################################################################

    identifier = "sleep_ms_duration_and_provisioning_factor_to_warm_function_percentage"
    arguments = []

    function_instance_mb_sizes = [128]
    invocation_counts = [1000]
    sleep_ms_durations = [0, 100, 200, 300, 400, 500, 600]
    provisioning_factors = [1.0, 1.05, 1.1, 1.15, 1.2, 1.25, 1.3]
    repetition_count = [10]

    arguments += list(
        itertools.product(*[
            function_instance_mb_sizes, invocation_counts, sleep_ms_durations, provisioning_factors, repetition_count
        ]))

    experiments.append(Experiment(identifier, executable, parameters, arguments))

    ####################################################################################################################
    # skyriseBenchmarkIdleAvailability
    ####################################################################################################################

    executable = "skyriseBenchmarkIdleAvailability"
    parameters = ["--function_instance_mb_sizes", "--invocation_counts", "--sleep_min_durations", "--repetition_count"]

    ####################################################################################################################
    # sleep_min_duration_to_availability_percentage
    ####################################################################################################################

    identifier = "sleep_min_duration_to_availability_percentage"
    arguments = []

    function_instance_mb_sizes = [128]
    invocation_counts = [1000]
    sleep_min_durations = [1]
    repetition_count = [1]

    arguments += list(
        itertools.product(*[function_instance_mb_sizes, invocation_counts, sleep_min_durations, repetition_count]))

    experiments.append(Experiment(identifier, executable, parameters, arguments))

    ####################################################################################################################
    # skyriseBenchmarkIdleLifetime
    ####################################################################################################################

    executable = "skyriseBenchmarkIdleLifetime"
    parameters = ["--function_instance_mb_sizes", "--invocation_counts", "--sleep_min_durations", "--repetition_count"]

    ####################################################################################################################
    # sleep_min_duration_to_idle_lifetime_percentage
    ####################################################################################################################

    identifier = "sleep_min_duration_to_idle_lifetime_percentage"
    arguments = []

    function_instance_mb_sizes = [128]
    invocation_counts = [1000]
    sleep_min_durations = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    repetition_count = [1]

    arguments += list(
        itertools.product(*[function_instance_mb_sizes, invocation_counts, sleep_min_durations, repetition_count]))

    # Example
    def plot_1(data):
        print(data)

    def plot_2(data):
        print(data)

    plots = {"plot_identifier_1": plot_1, "plot_identifier_2": plot_2}

    experiments.append(Experiment(identifier, executable, parameters, arguments, plots))

    ####################################################################################################################
    # skyriseBenchmarkInvocationLatency
    ####################################################################################################################

    executable = "skyriseBenchmarkInvocationLatency"
    parameters = [
        "--function_instance_mb_sizes", "--invocation_counts", "--warm_modes", "--sleep_ms_durations",
        "--repetition_count"
    ]

    ####################################################################################################################
    # warm_mode_to_network_call_and_initialization_total
    ####################################################################################################################

    identifier = "warm_mode_to_network_call_and_initialization_total"
    arguments = []

    function_instance_mb_sizes = [1000]
    invocation_counts = [10]
    warm_modes = ["false", "true"]
    sleep_ms_durations = [1000]
    repetition_count = [10]

    arguments += list(
        itertools.product(
            *[function_instance_mb_sizes, invocation_counts, warm_modes, sleep_ms_durations, repetition_count]))

    experiments.append(Experiment(identifier, executable, parameters, arguments))

    ####################################################################################################################
    # skyriseBenchmarkNetworkThroughput
    ####################################################################################################################

    executable = "skyriseBenchmarkNetworkThroughput"
    parameters = [
        "--function_instance_mb_sizes", "--object_byte_sizes", "--thread_counts", "--batch_size", "--repetition_count"
    ]

    ####################################################################################################################
    # function_instance_mb_size_and_thread_count_to_throughput_mb_per_s
    ####################################################################################################################

    identifier = "function_instance_mb_size_and_thread_count_to_throughput_mb_per_s"
    arguments = []

    function_instance_mb_sizes = [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]
    object_byte_sizes = [16777216]
    thread_counts = [4, 8, 16]
    batch_size = [100]
    repetition_count = [1]

    arguments += list(
        itertools.product(
            *[function_instance_mb_sizes, object_byte_sizes, thread_counts, batch_size, repetition_count]))

    experiments.append(Experiment(identifier, executable, parameters, arguments))
