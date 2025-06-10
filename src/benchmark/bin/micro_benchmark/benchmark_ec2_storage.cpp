#include <cstddef>
#include <exception>
#include <iostream>
#include <memory>
#include <vector>

#include <cxxopts.hpp>

#include "../benchmark_executable.hpp"
#include "micro_benchmark/ec2/ec2_benchmark_config.hpp"
#include "micro_benchmark/ec2/ec2_storage_benchmark.hpp"

int main(int argc, char* argv[]) {
  try {
    BenchmarkExecutable executable("ec2StorageBenchmark", "EC2 Storage Benchmark");

    cxxopts::OptionAdder& option_adder = executable.GetOptionAdder();
    option_adder("instance_types", "The instance types", cxxopts::value<std::vector<std::string>>());
    option_adder("concurrent_instance_counts",
                 "The concurrent instance counts. Up to 16 concurrent invocations are currently supported.",
                 cxxopts::value<std::vector<size_t>>());
    option_adder("repetition_count", "The repetition count", cxxopts::value<size_t>());
    option_adder("after_repetition_delays_min", "The after repetition delays [min]",
                 cxxopts::value<std::vector<size_t>>());
    option_adder("object_size_kb", "The size of the object to write and read", cxxopts::value<size_t>());
    option_adder("object_count", "The number of objects to write and read",
                 cxxopts::value<size_t>()->default_value("1"));
    option_adder("storage_types", "The storage types", cxxopts::value<std::vector<std::string>>());
    option_adder("enable_s3_eoz", "Enable S3 Express", cxxopts::value<bool>());
    option_adder("enable_vpc", "Enable VPC and AZ pinning (us-east-1a)", cxxopts::value<bool>());

    const cxxopts::ParseResult& parse_result = executable.GetParseResult(argc, argv);

    auto benchmark = std::make_shared<skyrise::Ec2StorageBenchmark>(
        skyrise::Ec2BenchmarkConfig::ParseInstanceTypes(parse_result["instance_types"].as<std::vector<std::string>>()),
        parse_result["concurrent_instance_counts"].as<std::vector<size_t>>(),
        parse_result["repetition_count"].as<size_t>(),
        parse_result["after_repetition_delays_min"].as<std::vector<size_t>>(),
        parse_result["object_size_kb"].as<size_t>(), parse_result["object_count"].as<size_t>(),
        skyrise::ParseStorageSystems(parse_result["storage_types"].as<std::vector<std::string>>()),
        parse_result["enable_s3_eoz"].as<bool>(), parse_result["enable_vpc"].as<bool>());
    executable.ExecuteBenchmark(benchmark);
  } catch (const std::exception& exception) {
    std::cout << exception.what() << "\n";

    return 1;
  }

  return 0;
}
