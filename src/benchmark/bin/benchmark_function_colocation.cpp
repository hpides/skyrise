#include "benchmark_executable.hpp"
#include "lambda/function_colocation_benchmark.hpp"

int main(int argc, char* argv[]) {
  try {
    BenchmarkExecutable executable("skyriseBenchmarkFunctionColocation", "Function Colocation Benchmark");

    cxxopts::OptionAdder& option_adder = executable.GetOptionAdder();
    option_adder("function_instance_mb_sizes", "The function instance sizes [MB]",
                 cxxopts::value<std::vector<size_t>>());
    option_adder("invocation_counts", "The invocation counts", cxxopts::value<std::vector<size_t>>());
    option_adder("sleep_min_durations", "The sleep durations [min]", cxxopts::value<std::vector<size_t>>());
    option_adder("repetition_count", "The repetition count", cxxopts::value<size_t>());

    const cxxopts::ParseResult& parse_result = executable.GetParseResult(argc, argv);

    auto benchmark = std::make_shared<skyrise::FunctionColocationBenchmark>(
        executable.GetCostCalculator(), parse_result["function_instance_mb_sizes"].as<std::vector<size_t>>(),
        parse_result["invocation_counts"].as<std::vector<size_t>>(),
        parse_result["sleep_min_durations"].as<std::vector<size_t>>(), parse_result["repetition_count"].as<size_t>());

    executable.ExecuteBenchmark(benchmark);
  } catch (const std::exception& exception) {
    std::cout << exception.what() << "\n";

    return 1;
  }

  return 0;
}
