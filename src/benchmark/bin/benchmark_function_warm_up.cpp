#include "benchmark_executable.hpp"
#include "function_warm_up_benchmark.hpp"

int main(int argc, char* argv[]) {
  try {
    BenchmarkExecutable executable("skyriseBenchmarkFunctionWarmUp", "Function Warm Up Benchmark");

    cxxopts::OptionAdder& option_adder = executable.GetOptionAdder();
    option_adder("function_instance_mb_sizes", "The function instance sizes [MB]",
                 cxxopts::value<std::vector<size_t>>());
    option_adder("invocation_counts", "The invocation counts", cxxopts::value<std::vector<size_t>>());
    option_adder("sleep_ms_durations", "The sleep durations [ms]", cxxopts::value<std::vector<size_t>>());
    option_adder("provisioning_factors", "The provisioning factors", cxxopts::value<std::vector<double>>());
    option_adder("enable_provisioned_concurrency", "Enable provisioned concurrency", cxxopts::value<bool>());
    option_adder("repetition_count", "The repetition count", cxxopts::value<size_t>());

    cxxopts::ParseResult& parse_result = executable.GetParseResult(argc, argv);

    auto benchmark = std::make_shared<skyrise::FunctionWarmUpBenchmark>(
        executable.GetCostCalculator(), parse_result["function_instance_mb_sizes"].as<std::vector<size_t>>(),
        parse_result["invocation_counts"].as<std::vector<size_t>>(),
        parse_result["sleep_ms_durations"].as<std::vector<size_t>>(),
        parse_result["provisioning_factors"].as<std::vector<double>>(),
        parse_result["enable_provisioned_concurrency"].as<bool>(), parse_result["repetition_count"].as<size_t>());

    executable.ExecuteBenchmark(benchmark);
  } catch (const std::exception& exception) {
    std::cout << exception.what() << "\n";

    return 1;
  }

  return 0;
}
