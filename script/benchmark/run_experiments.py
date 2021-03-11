import argparse
import enum
import json
import pathlib
import subprocess
import tempfile
import time

import pytictoc
import termcolor

import experiment_specifications


class InfoType(enum.Enum):
    DOUBLE_SEPARATOR = 1
    SINGLE_SEPARATOR = 2
    RUN = 3
    PASSED = 4
    FAILED = 5


info_type_texts = {
    InfoType.DOUBLE_SEPARATOR: "[==========]",
    InfoType.SINGLE_SEPARATOR: "[----------]",
    InfoType.RUN: "[ RUN      ]",
    InfoType.PASSED: "[  PASSED  ]",
    InfoType.FAILED: "[  FAILED  ]",
}


def print_info(info_type: InfoType, info: str = ""):
    print("%s %s" %
          (termcolor.colored(info_type_texts[info_type], "red" if info_type == InfoType.FAILED else "green"), info))


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

    experiments = [
        experiment for experiment in experiment_specifications.experiments
        if (experiment.identifier in experiment_identifiers)
    ] if experiment_identifiers else experiment_specifications.experiments

    script_path = pathlib.Path(__file__).parent.resolve()
    executable_path = pathlib.Path(script_path / experiment_specifications.executables_path).resolve()
    experiment_path = pathlib.Path(script_path / experiment_specifications.experiments_path /
                                   time.strftime("%Y_%m_%d_%H_%M_%S")).resolve()

    experiment_path.mkdir(parents=True, exist_ok=True)

    print_info(InfoType.DOUBLE_SEPARATOR,
               "Running %s experiment%s" % (len(experiments), "s" if len(experiments) > 1 else ""))
    print_info(InfoType.SINGLE_SEPARATOR)

    timer = pytictoc.TicToc()
    elapsed_times = []

    for experiment in experiments:
        print_info(InfoType.RUN, experiment.identifier)

        is_success = True
        timer.tic()

        output_path = pathlib.Path(experiment_path / ("%s_%s.json" %
                                                      (experiment.executable, experiment.identifier))).resolve()

        with output_path.open("w") as output_file:
            json_output = None

            for index, arguments in enumerate(experiment.arguments):
                with tempfile.NamedTemporaryFile() as temporary_file:
                    command = "%s %s" % (pathlib.Path(
                        executable_path / experiment.executable).resolve(), temporary_file.name)
                    for (parameter, argument) in zip(experiment.parameters, arguments):
                        command += " %s %s" % (parameter, argument)

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
                output_file.write(json.dumps(json_output, indent=2))

        elapsed_times.append(round(timer.tocvalue(), 2))

        print_info(InfoType.PASSED if is_success else InfoType.FAILED,
                   "%s (%ss)" % (experiment.identifier, elapsed_times[-1]))
        print_info(InfoType.SINGLE_SEPARATOR)

    print_info(
        InfoType.DOUBLE_SEPARATOR,
        "%s experiment%s ran (%ss)" % (len(experiments), "s" if len(experiments) > 1 else "", sum(elapsed_times)))
