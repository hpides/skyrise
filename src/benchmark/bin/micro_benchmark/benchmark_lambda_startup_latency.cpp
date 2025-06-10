#include <magic_enum/magic_enum.hpp>

#include "benchmark_executable.hpp"
#include "micro_benchmark/lambda/lambda_startup_latency_benchmark.hpp"

namespace {

std::vector<skyrise::DeploymentType> ParseDeploymentTypes(const std::vector<std::string>& deployment_types) {
  std::vector<skyrise::DeploymentType> result_types;
  result_types.reserve(deployment_types.size());

  std::ranges::transform(deployment_types, std::back_inserter(result_types), [](const std::string& instance_type) {
    return magic_enum::enum_cast<skyrise::DeploymentType>(instance_type).value();
  });

  return result_types;
}

}  // namespace

int main(int argc, char* argv[]) {
  try {
    BenchmarkExecutable executable("lambdaStartupLatencyBenchmark", "Lambda Startup Latency Benchmark");

    cxxopts::OptionAdder& option_adder = executable.GetOptionAdder();
    option_adder("function_instance_sizes_mb", "The function instance sizes [MB]",
                 cxxopts::value<std::vector<size_t>>());
    option_adder("concurrent_instance_counts",
                 "The concurrent instance counts; set concurrent_instance_counts to <= 100 and use repetition_count to "
                 "multiply the "
                 "number of traces",
                 cxxopts::value<std::vector<size_t>>());
    option_adder("repetition_count", "The repetition count", cxxopts::value<size_t>());
    option_adder("deployment_types", "The deployment types", cxxopts::value<std::vector<std::string>>());

    const cxxopts::ParseResult& parse_result = executable.GetParseResult(argc, argv);

    auto benchmark = std::make_shared<skyrise::LambdaStartupLatencyBenchmark>(
        executable.GetCostCalculator(), executable.GetClient().GetS3Client(), executable.GetClient().GetXRayClient(),
        parse_result["function_instance_sizes_mb"].as<std::vector<size_t>>(),
        parse_result["concurrent_instance_counts"].as<std::vector<size_t>>(),
        parse_result["repetition_count"].as<size_t>(),
        ParseDeploymentTypes(parse_result["deployment_types"].as<std::vector<std::string>>()));

    executable.ExecuteBenchmark(benchmark);
  } catch (const std::exception& exception) {
    std::cout << exception.what() << "\n";

    return 1;
  }

  return 0;
}
