#include "benchmark_executable.hpp"
#include "micro_benchmark/lambda/lambda_warmup_benchmark.hpp"

int main(int argc, char* argv[]) {
  try {
    BenchmarkExecutable executable("lambdaWarmupBenchmark", "Lambda Warmup Benchmark");

    cxxopts::OptionAdder& option_adder = executable.GetOptionAdder();
    option_adder("function_instance_sizes_mb", "The function instance sizes [MB]",
                 cxxopts::value<std::vector<size_t>>());
    option_adder("concurrent_instance_counts", "The concurrent instance counts", cxxopts::value<std::vector<size_t>>());
    option_adder("repetition_count", "The repetition count", cxxopts::value<size_t>());
    option_adder("enable_provisioned_concurrency", "Enable provisioned concurrency", cxxopts::value<bool>());
    option_adder("provisioning_factors", "The provisioning factors", cxxopts::value<std::vector<double>>());

    const cxxopts::ParseResult& parse_result = executable.GetParseResult(argc, argv);

    auto benchmark = std::make_shared<skyrise::LambdaWarmupBenchmark>(
        executable.GetCostCalculator(), parse_result["function_instance_sizes_mb"].as<std::vector<size_t>>(),
        parse_result["concurrent_instance_counts"].as<std::vector<size_t>>(),
        parse_result["repetition_count"].as<size_t>(), parse_result["enable_provisioned_concurrency"].as<bool>(),
        parse_result["provisioning_factors"].as<std::vector<double>>());

    executable.ExecuteBenchmark(benchmark);
  } catch (const std::exception& exception) {
    std::cout << exception.what() << "\n";

    return 1;
  }

  return 0;
}
