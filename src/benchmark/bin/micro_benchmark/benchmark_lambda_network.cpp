#include "benchmark_executable.hpp"
#include "micro_benchmark/lambda/lambda_network_benchmark.hpp"

namespace {

/**
 * The duration of the benchmark.
 */
constexpr size_t kDurationSeconds = 10;

/**
 * The iPerf reporting interval in milliseconds.
 * Defaults to 1,000 milliseconds, which gives a good indication of the initial burst and bucket size configuration.
 *
 * For more fine-granular reports, choose the shortest possible interval of 5 milliseconds.
 */
constexpr size_t kIperfReportIntervalMilliseconds = 1000;

/**
 * The iPerf message size in KB.
 */
constexpr int kIperfMessageSizeKb = 16;

}  // namespace

int main(int argc, char* argv[]) {
  try {
    BenchmarkExecutable executable("lambdaNetworkBenchmark", "Lambda Network Benchmark");

    cxxopts::OptionAdder& option_adder = executable.GetOptionAdder();
    option_adder("function_instance_sizes_mb", "The function instance sizes [MB]",
                 cxxopts::value<std::vector<size_t>>());
    option_adder("concurrent_instance_counts",
                 "The concurrent instance counts. Up to 16 concurrent invocations are currently supported.",
                 cxxopts::value<std::vector<size_t>>());
    option_adder("repetition_count", "The repetition count", cxxopts::value<size_t>());
    option_adder("duration_s", "The duration in seconds.",
                 cxxopts::value<size_t>()->default_value(std::to_string(kDurationSeconds)));
    option_adder("report_interval_ms", "The interval in milliseconds in which iPerf reports the throughput.",
                 cxxopts::value<size_t>()->default_value(std::to_string(kIperfReportIntervalMilliseconds)));
    option_adder("target_throughput_mbps", "The target throughput in MB/s. Default means unlimited.",
                 cxxopts::value<size_t>()->default_value(std::to_string(std::numeric_limits<size_t>::max())));
    option_adder("enable_download", "The Lambda function receives packages from EC2.",
                 cxxopts::value<bool>()->default_value(std::to_string(false)));
    option_adder("enable_upload", "The Lambda function sends packages to EC2.",
                 cxxopts::value<bool>()->default_value(std::to_string(false)));
    option_adder("distinct_function", "Use a distinct function for each repetition.",
                 cxxopts::value<bool>()->default_value(std::to_string(false)));
    option_adder("enable_vpc", "Enable VPC and AZ pinning (us-east-1a)", cxxopts::value<bool>());
    option_adder("message_size_kb", "The message size of the packages in KB.",
                 cxxopts::value<int>()->default_value(std::to_string(kIperfMessageSizeKb)));

    const cxxopts::ParseResult& parse_result = executable.GetParseResult(argc, argv);

    auto benchmark = std::make_shared<skyrise::LambdaNetworkBenchmark>(
        executable.GetCostCalculator(), executable.GetClient().GetEc2Client(), executable.GetClient().GetSqsClient(),
        parse_result["function_instance_sizes_mb"].as<std::vector<size_t>>(),
        parse_result["concurrent_instance_counts"].as<std::vector<size_t>>(),
        parse_result["repetition_count"].as<size_t>(), parse_result["duration_s"].as<size_t>(),
        parse_result["report_interval_ms"].as<size_t>(), parse_result["target_throughput_mbps"].as<size_t>(),
        parse_result["enable_download"].as<bool>(), parse_result["enable_upload"].as<bool>(),
        parse_result["distinct_function"].as<bool>() ? skyrise::DistinctFunctionPerRepetition::kYes
                                                     : skyrise::DistinctFunctionPerRepetition::kNo,
        parse_result["enable_vpc"].as<bool>(), parse_result["message_size_kb"].as<int>());

    executable.ExecuteBenchmark(benchmark);
  } catch (const std::exception& exception) {
    std::cout << exception.what() << "\n";

    return 1;
  }

  return 0;
}
