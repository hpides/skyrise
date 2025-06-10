#include "benchmark_executable.hpp"
#include "micro_benchmark/lambda/lambda_idletime_benchmark.hpp"

int main(int argc, char* argv[]) {
  try {
    BenchmarkExecutable executable("lambdaIdletimeBenchmark", "Lambda Idletime Benchmark");

    cxxopts::OptionAdder& option_adder = executable.GetOptionAdder();
    option_adder("function_instance_sizes_mb", "The function instance sizes [MB]",
                 cxxopts::value<std::vector<size_t>>());
    option_adder("concurrent_instance_counts", "The concurrent instance counts", cxxopts::value<std::vector<size_t>>());
    option_adder("repetition_count", "The repetition count", cxxopts::value<size_t>());
    option_adder("after_repetition_delays_min", "The after repetition delays [min]",
                 cxxopts::value<std::vector<size_t>>());

    const cxxopts::ParseResult& parse_result = executable.GetParseResult(argc, argv);

    auto benchmark = std::make_shared<skyrise::LambdaIdletimeBenchmark>(
        executable.GetCostCalculator(), parse_result["function_instance_sizes_mb"].as<std::vector<size_t>>(),
        parse_result["concurrent_instance_counts"].as<std::vector<size_t>>(),
        parse_result["repetition_count"].as<size_t>(),
        parse_result["after_repetition_delays_min"].as<std::vector<size_t>>());

    executable.ExecuteBenchmark(benchmark);
  } catch (const std::exception& exception) {
    std::cout << exception.what() << "\n";

    return 1;
  }

  return 0;
}
