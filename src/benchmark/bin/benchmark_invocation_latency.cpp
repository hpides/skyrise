#include "benchmark_executable.hpp"
#include "lambda/invocation_latency_benchmark.hpp"

int main(int argc, char* argv[]) {
  try {
    BenchmarkExecutable executable("skyriseBenchmarkInvocationLatency", "Invocation Latency Benchmark");

    cxxopts::OptionAdder& option_adder = executable.GetOptionAdder();
    option_adder("function_instance_mb_sizes", "The function instance sizes [MB]",
                 cxxopts::value<std::vector<size_t>>());
    option_adder("invocation_counts",
                 "The invocation counts; set invocation_count to <= 100 and use repetition_count to multiply the "
                 "number of traces",
                 cxxopts::value<std::vector<size_t>>());
    option_adder("warm_modes", "The warm modes", cxxopts::value<std::vector<bool>>());
    option_adder("sleep_ms_durations",
                 "The sleep durations [ms]; increase sleep_ms_duration if there are too many function warm starts",
                 cxxopts::value<std::vector<size_t>>());
    option_adder("repetition_count", "The repetition count", cxxopts::value<size_t>());

    const cxxopts::ParseResult& parse_result = executable.GetParseResult(argc, argv);

    auto benchmark = std::make_shared<skyrise::InvocationLatencyBenchmark>(
        executable.GetClient().GetXRayClient(), executable.GetBenchmarkHelper(), executable.GetCostCalculator(),
        parse_result["function_instance_mb_sizes"].as<std::vector<size_t>>(),
        parse_result["invocation_counts"].as<std::vector<size_t>>(), parse_result["warm_modes"].as<std::vector<bool>>(),
        parse_result["sleep_ms_durations"].as<std::vector<size_t>>(), parse_result["repetition_count"].as<size_t>());

    executable.ExecuteBenchmark(benchmark);
  } catch (const std::exception& exception) {
    std::cout << exception.what() << "\n";

    return 1;
  }

  return 0;
}
