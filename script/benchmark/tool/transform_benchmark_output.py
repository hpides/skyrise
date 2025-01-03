import argparse
import json


class GoogleBenchmark:

    def __init__(self,
                 name: str,
                 repetitions: int,
                 iterations: int,
                 real_time: float,
                 cpu_time: float,
                 time_unit: str,
                 repetition_index: int = 0,
                 **kwargs):
        self.name = name
        self.repetitions = repetitions
        self.repetition_index = repetition_index
        self.iterations = iterations
        self.real_time = real_time
        self.cpu_time = cpu_time
        self.time_unit = time_unit
        self.additional = kwargs


class GoogleBenchmarkContext:

    # noinspection PyUnusedLocal
    def __init__(self, date: str, host_name: str, executable: str, num_cpus: int, mhz_per_cpu: int,
                 cpu_scaling_enabled: bool, library_build_type: str, **kwargs):
        self.date = date
        self.host_name = host_name
        self.executable = executable
        self.cpu_cores = num_cpus
        self.cpu_mhz = mhz_per_cpu
        self.cpu_scaling_enabled = cpu_scaling_enabled
        self.build_type = library_build_type


class MicroBenchmark:

    def __init__(self):
        self.repetitions: list[GoogleBenchmark] = list()
        self.med = None
        self.stddev = None
        self.cv = None

    def json(self, ctx: GoogleBenchmarkContext, *args):
        return {
            "context": self._context(ctx),
            "scale": self.__scale(),
            "results": {
                "metrics":
                    self._metrics(args),
                f"real_times_{self.repetitions[0].time_unit}":
                    list(map(lambda b: round(b.real_time, 2), self.repetitions)),
                f"cpu_times_{self.repetitions[0].time_unit}":
                    list(map(lambda b: round(b.cpu_time, 2), self.repetitions)),
                **{
                    metric: list(map(lambda b: round(b.additional[metric], 2), self.repetitions)) for metric in args
                },
            }
        }

    def _context(self, ctx: GoogleBenchmarkContext):
        return {
            "benchmark_name": self.__benchmark_name(),
            "date": ctx.date,
            "executable": ctx.executable,
            "num_cpus": ctx.cpu_cores,
            "mhz_per_cpu": ctx.cpu_mhz,
            "cpu_scaling_enabled": ctx.cpu_scaling_enabled,
            "library_build_type": ctx.build_type
        }

    def _metrics(self, custom_metrics):
        if self.med is None or self.stddev is None or self.cv is None:
            return dict()

        return {
            f"real_times_{self.med.time_unit}_median": self.med.real_time,
            f"real_times_{self.stddev.time_unit}_stddev": self.stddev.real_time,
            f"real_times_{self.cv.time_unit}_cv": self.cv.real_time,
            f"cpu_times_{self.med.time_unit}_median": self.med.cpu_time,
            f"cpu_times_{self.stddev.time_unit}_stddev": self.stddev.cpu_time,
            f"cpu_times_{self.cv.time_unit}_cv": self.cv.cpu_time,
            **self.__add_custom_metrics(custom_metrics)
        }

    def __benchmark_name(self):
        return "_".join(self.__representative_signature().split("/")[:2])

    def __scale(self):
        return self.__representative_signature().split("/")[2]

    def __representative_signature(self):
        return self.repetitions[0].name

    def __add_custom_metrics(self, custom_metrics):
        metrics = dict()
        for metric in custom_metrics:
            metrics[f"{metric}_median"] = round(self.med.additional[metric], 2)
            metrics[f"{metric}_stddev"] = round(self.stddev.additional[metric], 2)
            metrics[f"{metric}_cv"] = round(self.cv.additional[metric], 2)
        return metrics


def extract_grouping_key(benchmark_name: str) -> str:
    return "/".join(benchmark_name.split("/")[:3])


def generate_output(input_file_location: str) -> dict:
    with open(input_file_location) as file:
        src = json.load(file)

        context = GoogleBenchmarkContext(**src["context"])
        micro_benchmarks = dict()

        for benchmark in src["benchmarks"]:

            key = extract_grouping_key(benchmark["name"])
            if micro_benchmarks.get(key) is None:
                micro_benchmarks[key] = MicroBenchmark()

            google_benchmark_output = GoogleBenchmark(**benchmark)
            if google_benchmark_output.name.endswith("_median"):
                micro_benchmarks[key].med = google_benchmark_output
            elif google_benchmark_output.name.endswith("_stddev"):
                micro_benchmarks[key].stddev = google_benchmark_output
            elif google_benchmark_output.name.endswith("_cv"):
                micro_benchmarks[key].cv = google_benchmark_output
            else:
                micro_benchmarks[key].repetitions.append(google_benchmark_output)

        return {
            benchmark: micro_benchmarks[benchmark].json(context, "bytes_per_second")
            for benchmark in micro_benchmarks.keys()
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Transform output of Google-Benchmarks into better readable summaries.')
    parser.add_argument('--in',
                        dest='input_file_location',
                        type=str,
                        required=True,
                        help='file location of input file.')
    parser.add_argument('--out',
                        dest='output_file_location',
                        type=str,
                        required=True,
                        help='location for file to be produced by transformer.')

    args = parser.parse_args()

    benchmark_output = generate_output(args.input_file_location)

    with open(args.output_file_location, mode="w") as output_file:
        json.dump(benchmark_output, output_file)
