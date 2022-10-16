#include <algorithm>

#include <magic_enum.hpp>

#include "benchmark_executable.hpp"
#include "ec2/ec2_invocation_benchmark.hpp"

namespace {

std::vector<skyrise::Ec2InstanceType> ParseInstanceTypes(const std::vector<std::string>& instance_types) {
  std::vector<skyrise::Ec2InstanceType> result_types;
  result_types.reserve(instance_types.size());

  std::transform(instance_types.cbegin(), instance_types.cend(), std::back_inserter(result_types),
                 [](const std::string& instance_type) {
                   return magic_enum::enum_cast<skyrise::Ec2InstanceType>(instance_type).value();
                 });

  return result_types;
}

}  // namespace

int main(int argc, char* argv[]) {
  try {
    BenchmarkExecutable executable("skyriseBenchmarkEc2Invocation", "EC2 Invocation Benchmark");

    cxxopts::OptionAdder& option_adder = executable.GetOptionAdder();
    option_adder("concurrent_invocation_counts", "The concurrent invocation counts",
                 cxxopts::value<std::vector<size_t>>());
    option_adder("instance_types", "The instance types", cxxopts::value<std::vector<std::string>>());
    option_adder("repetition_count", "The repetition count", cxxopts::value<size_t>());

    const cxxopts::ParseResult& parse_result = executable.GetParseResult(argc, argv);

    auto benchmark = std::make_shared<skyrise::Ec2InvocationBenchmark>(
        parse_result["concurrent_invocation_counts"].as<std::vector<size_t>>(),
        ParseInstanceTypes(parse_result["instance_types"].as<std::vector<std::string>>()),
        parse_result["repetition_count"].as<size_t>());

    executable.ExecuteBenchmark(benchmark);
  } catch (const std::exception& exception) {
    std::cout << exception.what() << "\n";

    return 1;
  }

  return 0;
}
