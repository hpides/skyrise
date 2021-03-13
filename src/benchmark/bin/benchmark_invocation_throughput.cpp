#include "benchmark_executable.hpp"
#include "invocation_throughput_benchmark.hpp"

int main(int argc, char* argv[]) {
  try {
    BenchmarkExecutable executable("skyriseBenchmarkInvocationThroughput", "Invocation Throughput Benchmark");

    cxxopts::OptionAdder& option_adder = executable.GetOptionAdder();
    option_adder("function_instance_mb_sizes", "The function instance sizes [MB]",
                 cxxopts::value<std::vector<size_t>>());
    option_adder("invocation_counts", "The invocation counts", cxxopts::value<std::vector<size_t>>());
    option_adder("function_payload_byte_sizes", "The function payload sizes [B]",
                 cxxopts::value<std::vector<size_t>>());
    option_adder("repetition_count", "The repetition count", cxxopts::value<size_t>());

    cxxopts::ParseResult& parse_result = executable.GetParseResult(argc, argv);

    auto benchmark = std::make_shared<skyrise::InvocationThroughputBenchmark>(
        executable.GetCostCalculator(), parse_result["function_instance_mb_sizes"].as<std::vector<size_t>>(),
        parse_result["invocation_counts"].as<std::vector<size_t>>(),
        parse_result["function_payload_byte_sizes"].as<std::vector<size_t>>(),
        parse_result["repetition_count"].as<size_t>());

    executable.ExecuteBenchmark(benchmark);
  } catch (const std::exception& exception) {
    std::cout << exception.what() << "\n";

    return 1;
  }

  return 0;
}
