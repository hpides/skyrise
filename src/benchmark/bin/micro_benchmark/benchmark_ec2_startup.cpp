#include "../benchmark_executable.hpp"
#include "micro_benchmark/ec2/ec2_benchmark_config.hpp"
#include "micro_benchmark/ec2/ec2_startup_benchmark.hpp"

int main(int argc, char* argv[]) {
  try {
    BenchmarkExecutable executable("ec2StartupBenchmark", "EC2 Startup Benchmark");

    cxxopts::OptionAdder& option_adder = executable.GetOptionAdder();
    option_adder("instance_types", "The instance types", cxxopts::value<std::vector<std::string>>());
    option_adder("concurrent_instance_counts", "The concurrent instance counts", cxxopts::value<std::vector<size_t>>());
    option_adder("repetition_count", "The repetition count", cxxopts::value<size_t>());
    option_adder("warmup_flag", "The warmup flag", cxxopts::value<bool>()->default_value(std::to_string(true)));

    const cxxopts::ParseResult& parse_result = executable.GetParseResult(argc, argv);

    auto benchmark = std::make_shared<skyrise::Ec2StartupBenchmark>(
        skyrise::Ec2BenchmarkConfig::ParseInstanceTypes(parse_result["instance_types"].as<std::vector<std::string>>()),
        parse_result["concurrent_instance_counts"].as<std::vector<size_t>>(),
        parse_result["repetition_count"].as<size_t>(), parse_result["warmup_flag"].as<bool>());

    executable.ExecuteBenchmark(benchmark);
  } catch (const std::exception& exception) {
    std::cout << exception.what() << "\n";

    return 1;
  }

  return 0;
}
