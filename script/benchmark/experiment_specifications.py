import itertools

from typing import List, Union


class Experiment:

    def __init__(self, identifier: str, executable: str, parameters: List[str], arguments: List[Union[float, int,
                                                                                                      str]]):

        self.identifier = identifier
        self.executable = executable
        self.parameters = parameters
        self.arguments = arguments


def init():
    global executables_path
    executables_path = "../../cmake-build-release/bin"

    global experiments_path
    experiments_path = "../../experiments"

    global experiments
    experiments = []

    ####################################################################################################################
    # skyriseBenchmarkInvocationThroughput
    ####################################################################################################################

    executable = "skyriseBenchmarkInvocationThroughput"
    parameters = ["--function_instance_mb_sizes", "--invocation_counts", "--function_payload_byte_sizes"]

    ####################################################################################################################
    # invocation_count_to_invocation_throughput
    ####################################################################################################################

    identifier = "invocation_count_to_invocation_throughput"
    arguments = []

    function_instance_mb_sizes = [128]
    invocation_counts = [64, 128, 256, 512, 1024, 2048]
    function_payload_byte_sizes = [0]

    arguments += list(itertools.product(*[function_instance_mb_sizes, invocation_counts, function_payload_byte_sizes]))

    experiments.append(Experiment(identifier, executable, parameters, arguments))
