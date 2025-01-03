import argparse
import json
import pathlib
import subprocess
import tempfile
import time
import pandas
import pytictoc

from info_type import InfoType, print_info

import experiment_specifications

if __name__ == "__main__":
    experiment_specifications.init()

    parser = argparse.ArgumentParser(description="run Skyrise benchmark experiments",
                                     formatter_class=lambda prog: argparse.HelpFormatter(prog, max_help_position=60))
    parser.add_argument("-e",
                        "--experiments",
                        metavar="EXPERIMENTS",
                        help="the experiments",
                        action="store",
                        type=str,
                        default="",
                        dest="experiments")
    arguments = parser.parse_args()

    experiment_identifiers = set(filter(None, arguments.experiments.split(",")))

    experiments = experiment_specifications.experiments
    if experiment_identifiers:
        experiments = [
            experiment for experiment in experiment_specifications.experiments
            if (experiment.identifier in experiment_identifiers)
        ]

    script_path = pathlib.Path(__file__).parent.resolve()
    executable_path = script_path / experiment_specifications.executables_path
    experiment_path = script_path / experiment_specifications.experiments_path / time.strftime("%Y_%m_%d_%H_%M_%S")

    experiment_path.mkdir(parents=True, exist_ok=True)

    print_info(InfoType.DOUBLE_SEPARATOR, f"Running {len(experiments)} experiment{'s' if len(experiments) > 1 else ''}")
    print_info(InfoType.SINGLE_SEPARATOR)

    timer = pytictoc.TicToc()
    elapsed_times = []

    for experiment in experiments:
        print_info(InfoType.RUN, experiment.identifier)

        is_success = True
        json_output = {}
        timer.tic()

        for index, arguments in enumerate(experiment.arguments):
            with tempfile.NamedTemporaryFile() as temporary_file:
                command = f"{executable_path / experiment.executable} {temporary_file.name}"
                for (parameter, argument) in zip(experiment.parameters, arguments):
                    command += f" {parameter} {argument}"

                output = subprocess.run(command, stdout=subprocess.PIPE, universal_newlines=True, shell=True)

                if output.returncode == 0:
                    temporary_json_output = json.loads(temporary_file.read())

                    if index == 0:
                        json_output = temporary_json_output
                    else:
                        json_output["benchmarks"].extend(temporary_json_output["benchmarks"])
                else:
                    is_success = False
                    break

        if is_success and json_output:
            output_filename = f"{experiment.executable}_{experiment.identifier}"

            with (experiment_path / f"{output_filename}.json").open("w") as output_file:
                output_file.write(json.dumps(json_output, indent=2))

            csv_rows = []
            for benchmark_json_output in json_output["benchmarks"]:
                benchmark_json_output.pop("repetitions", None)

                csv_rows.append(pandas.json_normalize(benchmark_json_output))

            csv_output = pandas.concat(csv_rows)
            csv_output.to_csv(experiment_path / f"{output_filename}.csv", index=False)

        elapsed_times.append(round(timer.tocvalue(), 2))

        print_info(InfoType.PASSED if is_success else InfoType.FAILED,
                   f"{experiment.identifier} ({elapsed_times[-1]}s)")
        print_info(InfoType.SINGLE_SEPARATOR)

    print_info(InfoType.DOUBLE_SEPARATOR,
               f"{len(experiments)} experiment{'s' if len(experiments) > 1 else ''} ran ({sum(elapsed_times)}s)")
