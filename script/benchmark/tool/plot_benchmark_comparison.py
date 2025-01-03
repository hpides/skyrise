import argparse
from matplotlib import pyplot as plt
import numpy as np
from numpy import median

from transform_benchmark_output import generate_output


def plot_metric(metric: str, output_dir: str, benchmark_left: str, benchmark_right: str) -> None:
    data_left, data_right, maximum = extract_values_for_metric(benchmark_left, benchmark_right, metric)

    width = 0.25
    x_indices = np.arange(len(data_left))

    plt.clf()

    plt.bar(x_indices, data_left.values(), label=benchmark_left, width=width)
    plt.bar(x_indices + width, data_right.values(), label=benchmark_right, width=width)
    plt.xticks(x_indices + width / 2, data_right.keys())
    plt.ylim(0, maximum * 1.2)
    plt.legend()
    plt.title(metric.replace("_", " "))
    plt.annotate("Median Difference: " + compute_median_diff(data_left, data_right), (0.5, maximum))

    plt.savefig(f'{output_dir}/{metric}.png')


def compute_median_diff(data_left: dict, data_right: dict) -> str:
    median_diff = median([data_right[scale] / data_left[scale] for scale in data_right.keys()])

    return f"-{round(100 * (1 - median_diff), 2)}%" if median_diff < 1 else f"+{round(100 * (median_diff - 1), 2)}%"


def extract_values_for_metric(benchmark_left: str, benchmark_right: str, metric: str) -> (dict, dict, float):
    data_left = dict()
    data_right = dict()
    maximum = 0

    for benchmark_identifier in benchmark_output.keys():
        parts = benchmark_identifier.split("/")

        metric_val = benchmark_output[benchmark_identifier]["results"]["metrics"][metric]
        if parts[1] == benchmark_left:
            data_left[parts[2]] = metric_val
        elif parts[1] == benchmark_right:
            data_right[parts[2]] = metric_val
        maximum = max(maximum, metric_val)

    return data_left, data_right, maximum


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--in', dest='input_file_location', type=str, required=True)
    parser.add_argument('--out', dest='output_directory', type=str, required=True)
    parser.add_argument('--metrics', dest='metrics', nargs='+', type=str, required=True)
    parser.add_argument('--benchmark_left', dest='benchmark_left', type=str, required=True)
    parser.add_argument('--benchmark_right', dest='benchmark_right', type=str, required=True)

    args = parser.parse_args()

    benchmark_output = generate_output(args.input_file_location)

    for m in args.metrics:
        plot_metric(m, args.output_directory, args.benchmark_left, args.benchmark_right)
