#include "benchmark_executable.hpp"
#include "network_throughput_parallel_benchmark.hpp"

int main(int argc, char* argv[]) {
  try {
    BenchmarkExecutable executable("skyriseBenchmarkNetworkThroughputParallel",
                                   "Network Throughput Parallel Benchmark");

    cxxopts::OptionAdder& option_adder = executable.GetOptionAdder();
    option_adder("function_instance_mb_sizes", "The function instance sizes [MB]",
                 cxxopts::value<std::vector<size_t>>());
    option_adder("object_byte_sizes", "The object sizes [B]", cxxopts::value<std::vector<size_t>>());
    option_adder("batch_sizes", "The batch size", cxxopts::value<std::vector<size_t>>());
    option_adder("thread_counts", "The thread counts", cxxopts::value<std::vector<size_t>>());
    option_adder("invocation_counts", "The invocation counts", cxxopts::value<std::vector<size_t>>());
    option_adder("bucket_counts", "The bucket counts", cxxopts::value<std::vector<size_t>>());
    option_adder("enable_reads", "Enable reads", cxxopts::value<bool>());
    option_adder("repetition_count", "The repetition count", cxxopts::value<size_t>());

    cxxopts::ParseResult& parse_result = executable.GetParseResult(argc, argv);

    auto benchmark = std::make_shared<skyrise::NetworkThroughputParallelBenchmark>(
        executable.GetBenchmarkHelper(), executable.GetCostCalculator(),
        parse_result["function_instance_mb_sizes"].as<std::vector<size_t>>(),
        parse_result["object_byte_sizes"].as<std::vector<size_t>>(),
        parse_result["batch_sizes"].as<std::vector<size_t>>(), parse_result["thread_counts"].as<std::vector<size_t>>(),
        parse_result["invocation_counts"].as<std::vector<size_t>>(),
        parse_result["bucket_counts"].as<std::vector<size_t>>(), parse_result["enable_reads"].as<bool>(),
        parse_result["repetition_count"].as<size_t>());

    executable.ExecuteBenchmark(benchmark);
  } catch (const std::exception& exception) {
    std::cout << exception.what() << "\n";

    return 1;
  }

  return 0;
}
