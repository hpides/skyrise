#include <iostream>
#include <string>

#include <aws/core/Aws.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <cxxopts.hpp>

#include "benchmark_helper.hpp"
#include "benchmark_runner.hpp"
#include "client/client.hpp"
#include "network_throughput_parallel_benchmark.hpp"
#include "utils/costs/cost_calculator.hpp"
#include "utils/filesystem.hpp"
#include "utils/git_metadata.hpp"
#include "utils/time.hpp"

int main(int argc, char* argv[]) {
  int return_code = 0;

  cxxopts::ParseResult cli_arguments;
  Aws::SDKOptions sdk_options;
  sdk_options.httpOptions.installSigPipeHandler = true;

  try {
    // Parse the command line arguments
    cxxopts::Options cli_options("skyriseBenchmarkNetworkThroughputParallel", "Network Throughput Parallel Benchmark");

    cxxopts::OptionAdder cli_options_adder = cli_options.add_options();
    cli_options_adder("output", "The output file <file.json>", cxxopts::value<std::string>());

    cli_options_adder("invocation_counts", "The invocation counts", cxxopts::value<std::vector<size_t>>());
    cli_options_adder("batch_size", "The batch size", cxxopts::value<size_t>());
    cli_options_adder("repetition_count", "The repetition count", cxxopts::value<size_t>());

    cli_options_adder("verbose", "Show the verbose status log", cxxopts::value<bool>());
    cli_options_adder("help", "Print the usage overview", cxxopts::value<bool>());

    cli_options.parse_positional({"output"});
    cli_options.positional_help("OUTPUT");

    cli_arguments = cli_options.parse(argc, argv);

    if (cli_arguments.count("help") > 0) {
      std::cout << cli_options.help();
      exit(0);
    }

    if (cli_arguments.count("output") == 0) {
      throw cxxopts::option_required_exception("OUTPUT");
    }

    if (cli_arguments.count("verbose") > 0) {
      Aws::Utils::Logging::LogLevel log_level{Aws::Utils::Logging::LogLevel::Info};
      sdk_options.loggingOptions.logLevel = log_level;
      sdk_options.loggingOptions.logger_create_fn = [log_level]() {
        return Aws::MakeShared<Aws::Utils::Logging::ConsoleLogSystem>("console_logger", log_level);
      };
    }

    Aws::InitAPI(sdk_options);

    // Initialize the clients
    const auto client = std::make_shared<skyrise::Client>();
    const auto cost_calculator = std::make_shared<skyrise::CostCalculator>(client);
    const auto benchmark_runner = std::make_shared<skyrise::BenchmarkRunner>(client);
    const auto benchmark_helper = std::make_shared<skyrise::BenchmarkHelper>(client);

    // Initialize the benchmark
    skyrise::NetworkThroughputParallelBenchmark benchmark(
        benchmark_helper, cost_calculator, {cli_arguments["invocation_counts"].as<std::vector<size_t>>()},
        cli_arguments["batch_size"].as<size_t>(), cli_arguments["repetition_count"].as<size_t>());

    // Run the benchmark
    const auto benchmark_result = benchmark.Run(benchmark_runner);

    // Generate the output
    const auto output =
        Aws::Utils::Json::JsonValue()
            .WithObject("context", Aws::Utils::Json::JsonValue()
                                       .WithString("date", skyrise::GetFormattedTimestamp("%Y/%m/%d-%H:%M:%S"))
                                       .WithString("commit", GitMetadata::CommitSha1()))
            .WithArray("benchmarks", benchmark_result);

    // Save the output
    skyrise::WriteStringToFile(output.View().WriteReadable(), cli_arguments["output"].as<std::string>());

  } catch (const std::exception& exception) {
    std::cout << exception.what() << "\n";

    return_code = 1;
  }

  Aws::ShutdownAPI(sdk_options);

  return return_code;
}
