#include "../benchmark_executable.hpp"
#include "system_benchmark/system_benchmark.hpp"
#include "system_benchmark/system_benchmark_utils.hpp"

int main(int argc, char** argv) {
  try {
    BenchmarkExecutable executable("systemBenchmark", "System Benchmark");

    cxxopts::OptionAdder& option_adder = executable.GetOptionAdder();
    option_adder("compiler_name", "The name of the compiler [kTpch, kTpcxbb, ...]", cxxopts::value<std::string>());
    option_adder("query_id", "The IDs of the queries [kTpchQ1, kTpchQ6, ...]", cxxopts::value<std::string>());
    option_adder("scale_factor", "The scale factor of the dataset [1, 10, ...]", cxxopts::value<std::string>());
    option_adder("concurrent_instance_count", "The concurrent instance count", cxxopts::value<size_t>());
    option_adder("repetition_count", "The repetition count", cxxopts::value<size_t>());
    option_adder("after_repetition_delays_min", "The after repetition delays [min]",
                 cxxopts::value<std::vector<size_t>>());

    const cxxopts::ParseResult& parse_result = executable.GetParseResult(argc, argv);

    auto benchmark = std::make_shared<skyrise::SystemBenchmark>(
        executable.GetClient().GetIamClient(), executable.GetClient().GetLambdaClient(),
        executable.GetClient().GetS3Client(),
        skyrise::ParseCompilerName(parse_result["compiler_name"].as<std::string>()),
        skyrise::ParseQueryId(parse_result["query_id"].as<std::string>()),
        skyrise::ParseScaleFactor(parse_result["scale_factor"].as<std::string>()),
        parse_result["concurrent_instance_count"].as<size_t>(), parse_result["repetition_count"].as<size_t>(),
        parse_result["after_repetition_delays_min"].as<std::vector<size_t>>());
    executable.ExecuteBenchmark(benchmark);
  } catch (const std::exception& exception) {
    std::cout << exception.what() << "\n";

    return 1;
  }
  return 0;
}
