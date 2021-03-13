#include "benchmark_executable.hpp"
#include "network_latency_benchmark.hpp"

int main(int argc, char* argv[]) {
  try {
    BenchmarkExecutable executable("skyriseBenchmarkNetworkLatency", "Network Latency Benchmark");

    cxxopts::OptionAdder& option_adder = executable.GetOptionAdder();
    option_adder("function_instance_mb_sizes", "The function instance sizes [MB]",
                 cxxopts::value<std::vector<size_t>>());
    option_adder("object_byte_sizes_read", "The read object sizes [B]", cxxopts::value<std::vector<size_t>>());
    option_adder("object_byte_sizes_write", "The write  object sizes [B]", cxxopts::value<std::vector<size_t>>());
    option_adder("batch_size", "The batch size", cxxopts::value<size_t>());
    option_adder("repetition_count", "The repetition count", cxxopts::value<size_t>());

    cxxopts::ParseResult& parse_result = executable.GetParseResult(argc, argv);

    auto benchmark = std::make_shared<skyrise::NetworkLatencyBenchmark>(
        executable.GetBenchmarkHelper(), executable.GetCostCalculator(),
        parse_result["function_instance_mb_sizes"].as<std::vector<size_t>>(),
        parse_result["object_byte_sizes_read"].as<std::vector<size_t>>(),
        parse_result["object_byte_sizes_write"].as<std::vector<size_t>>(), parse_result["batch_size"].as<size_t>(),
        parse_result["repetition_count"].as<size_t>());

    executable.ExecuteBenchmark(benchmark);
  } catch (const std::exception& exception) {
    std::cout << exception.what() << "\n";

    return 1;
  }

  return 0;
}
