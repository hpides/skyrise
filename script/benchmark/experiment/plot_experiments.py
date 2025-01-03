import argparse
import matplotlib.pyplot as pyplot
import pandas
import pathlib

from info_type import InfoType, print_info

import experiment_specifications

if __name__ == "__main__":
    experiment_specifications.init()

    parser = argparse.ArgumentParser(description="plot Skyrise benchmark experiments",
                                     formatter_class=lambda prog: argparse.HelpFormatter(prog, max_help_position=60))
    parser.add_argument(metavar="RUN", help="the run", action="store", type=str, default="", dest="run")
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
    run_path = script_path / experiment_specifications.experiments_path / arguments.run

    print_info(InfoType.DOUBLE_SEPARATOR,
               f"Plotting {len(experiments)} experiment{'s' if len(experiments) > 1 else ''}")
    print_info(InfoType.SINGLE_SEPARATOR)

    for experiment in experiments:
        print_info(InfoType.PLOT, experiment.identifier)

        input_path = run_path / f"{experiment.executable}_{experiment.identifier}.csv"

        status = InfoType.PASSED

        if input_path.is_file() and experiment.plots:
            try:
                csv_input = pandas.read_csv(input_path, header=0)

                for plot_identifier, plot_function in experiment.plots.items():
                    plot_function(csv_input)

                    output_path = run_path / f"{experiment.executable}_{experiment.identifier}_{plot_identifier}.pdf"

                    pyplot.tight_layout()
                    pyplot.savefig(output_path, format="pdf")
                    pyplot.close()
            except Exception:
                status = InfoType.FAILED
        else:
            status = InfoType.SKIPPED

        print_info(status, experiment.identifier)
        print_info(InfoType.SINGLE_SEPARATOR)

    print_info(InfoType.DOUBLE_SEPARATOR, f"{len(experiments)} experiment{'s' if len(experiments) > 1 else ''} plotted")
