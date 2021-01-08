#include <ctime>
#include <iostream>
#include <map>
#include <string>
#include <utility>

#include <aws/core/Aws.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <cxxopts.hpp>
#include <termcolor/termcolor.hpp>

#include "benchmark.hpp"
#include "benchmark_config.hpp"
#include "benchmark_helper.hpp"
#include "benchmark_runner.hpp"
#include "client/client_aws.hpp"
#include "function_colocation_benchmark.hpp"
#include "idle_availability_benchmark.hpp"
#include "idle_lifetime_benchmark.hpp"
#include "invocation_throughput_benchmark.hpp"
#include "network_latency_benchmark.hpp"
#include "network_throughput_benchmark.hpp"
#include "network_throughput_parallel_benchmark.hpp"
#include "utils/array.hpp"
#include "utils/costs/cost_calculator.hpp"
#include "utils/filesystem.hpp"
#include "utils/git_metadata.hpp"
#include "utils/map.hpp"
#include "utils/random.hpp"
#include "utils/string.hpp"
#include "utils/time.hpp"
#include "utils/unit_conversion.hpp"
#include "utils/vector.hpp"

enum class ConsoleInfoType { kDoubleSeparator, kSingleSeparator, kRun, kPassed, kFailed };

const std::map<ConsoleInfoType, std::string> kConsoleInfoTypeText{{ConsoleInfoType::kDoubleSeparator, "[==========]"},
                                                                  {ConsoleInfoType::kSingleSeparator, "[----------]"},
                                                                  {ConsoleInfoType::kRun, "[ RUN      ]"},
                                                                  {ConsoleInfoType::kPassed, "[  PASSED  ]"},
                                                                  {ConsoleInfoType::kFailed, "[  FAILED  ]"}};

void PrintConsoleInfo(ConsoleInfoType info_type, const std::string& info = "") {
  std::cout << (info_type == ConsoleInfoType::kFailed ? termcolor::red : termcolor::green)
            << kConsoleInfoTypeText.at(info_type) << termcolor::reset << " " << info << "\n";
}

class BenchmarkRegistry {
 public:
  void RegisterBenchmark(const std::string& name, std::unique_ptr<skyrise::Benchmark> benchmark) {
    benchmarks_.emplace(name, std::move(benchmark));
  }

  [[nodiscard]] std::vector<std::string> GetRegisteredBenchmarkNames() const { return ExtractMapKeys(benchmarks_); }

  [[nodiscard]] const std::unique_ptr<skyrise::Benchmark>& GetBenchmark(const std::string& name) const {
    return benchmarks_.at(name);
  }

 private:
  std::map<std::string, std::unique_ptr<skyrise::Benchmark>> benchmarks_;
};

int main(int argc, char* argv[]) {
  int return_code = 0;

  cxxopts::ParseResult cli_arguments;
  Aws::SDKOptions sdk_options;

  try {
    // Parse the command line arguments
    cxxopts::Options cli_options("skyriseBenchmarkConsole", "Console for running Skyrise benchmarks");

    cxxopts::OptionAdder cli_options_adder = cli_options.add_options();
    cli_options_adder("output", "The output file <file.json>", cxxopts::value<std::string>());
    cli_options_adder("filter", "The benchmark filter <benchmark_1,...,benchmark_n>",
                      cxxopts::value<std::vector<std::string>>());
    cli_options_adder("shuffle", "Shuffle the benchmark execution order", cxxopts::value<bool>());
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
    const auto aws_client = std::make_shared<skyrise::ClientAws>();
    const auto benchmark_runner = std::make_shared<skyrise::BenchmarkRunner>(aws_client);
    const auto benchmark_helper = std::make_shared<skyrise::BenchmarkHelper>(aws_client);
    const auto cost_calculator = std::make_shared<skyrise::CostCalculator>(aws_client);

    // Register the benchmarks
    BenchmarkRegistry benchmark_registry;

    // Register FunctionColocationBenchmark
    benchmark_registry.RegisterBenchmark("FunctionColocationBenchmark",
                                         std::make_unique<skyrise::FunctionColocationBenchmark>(
                                             std::vector<size_t>{128, 2048}, std::vector<size_t>{512}, 1, 2));

    // Register IdleLifetimeBenchmark
    benchmark_registry.RegisterBenchmark(
        "IdleLifetimeBenchmark",
        std::make_unique<skyrise::IdleLifetimeBenchmark>(std::vector<size_t>{128}, std::vector<size_t>{4096},
                                                         std::vector<size_t>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}));

    // Register IdleAvailabilityBenchmark
    benchmark_registry.RegisterBenchmark("IdleAvailabilityBenchmark",
                                         std::make_unique<skyrise::IdleAvailabilityBenchmark>(
                                             std::vector<size_t>{128}, std::vector<size_t>{4096}, 1, 12));

    // Register InvocationThroughputBenchmark
    benchmark_registry.RegisterBenchmark(
        "InvocationThroughputBenchmark",
        std::make_unique<skyrise::InvocationThroughputBenchmark>(
            std::vector<size_t>{128}, std::vector<size_t>{8192},
            std::vector<skyrise::ExecuteMode>{skyrise::ExecuteMode::kWarmParallel, skyrise::ExecuteMode::kWarmAsync},
            std::vector<size_t>{skyrise::MbToByte(0)}));

    // Register NetworkLatencyBenchmark
    benchmark_registry.RegisterBenchmark(
        "NetworkLatencyBenchmark",
        std::make_unique<skyrise::NetworkLatencyBenchmark>(
            benchmark_helper, cost_calculator, std::vector<size_t>{128}, std::vector<size_t>{skyrise::KbToByte(1)},
            std::vector<size_t>{skyrise::KbToByte(1), skyrise::MbToByte(100)}, 10000));

    // Register NetworkThroughputBenchmark
    // TODO(d-justen): Utilize bigger function instances and more threads as well
    benchmark_registry.RegisterBenchmark(
        "NetworkThroughputBenchmark",
        std::make_unique<skyrise::NetworkThroughputBenchmark>(
            benchmark_helper, cost_calculator, std::vector<size_t>{128, 256, 512, 1024, 1792, 2048, 3008},
            std::vector<size_t>{skyrise::MbToByte(1), skyrise::MbToByte(2), skyrise::MbToByte(4), skyrise::MbToByte(8),
                                skyrise::MbToByte(16), skyrise::MbToByte(32), skyrise::MbToByte(64),
                                skyrise::MbToByte(128), skyrise::MbToByte(256)},
            std::vector<size_t>{1, 2, 4, 8}, 1000));

    // Register NetworkThroughputParallelBenchmark
    benchmark_registry.RegisterBenchmark(
        "NetworkThroughputParallelBenchmark",
        std::make_unique<skyrise::NetworkThroughputParallelBenchmark>(
            benchmark_helper, cost_calculator, std::vector<size_t>{1, 10, 100, 1000, 10000}, 100));

    // Filter the benchmarks (optional)
    std::vector<std::string> benchmark_names = benchmark_registry.GetRegisteredBenchmarkNames();
    if (cli_arguments.count("filter") > 0) {
      const auto filter_names = cli_arguments["filter"].as<std::vector<std::string>>();

      if (!skyrise::IsSubset(filter_names, benchmark_names)) {
        throw std::invalid_argument("Option ‘filter’ has an invalid value\n\nPossible values are subsets of " +
                                    skyrise::VectorToString(benchmark_names, ", "));
      }

      benchmark_names = filter_names;
    }

    // Shuffle the benchmarks (optional)
    if (cli_arguments.count("shuffle") > 0) {
      std::shuffle(benchmark_names.begin(), benchmark_names.end(), skyrise::RandomGenerator<std::mt19937>());
    }

    PrintConsoleInfo(ConsoleInfoType::kDoubleSeparator, "Running " + std::to_string(benchmark_names.size()) +
                                                            " benchmark" + (benchmark_names.size() > 1 ? "s" : ""));
    PrintConsoleInfo(ConsoleInfoType::kSingleSeparator);

    size_t total_duration = 0;

    // Run the benchmarks
    Aws::Utils::Array<Aws::Utils::Array<Aws::Utils::Json::JsonValue>> benchmark_results(benchmark_names.size());

    for (size_t i = 0; i < benchmark_names.size(); ++i) {
      PrintConsoleInfo(ConsoleInfoType::kRun, benchmark_names[i]);

      bool is_success = true;

      const auto start_time = std::chrono::steady_clock::now();

      try {
        benchmark_results[i] = benchmark_registry.GetBenchmark(benchmark_names[i])->Run(benchmark_runner);
      } catch (const std::exception& exception) {
        std::cout << exception.what() << "\n";

        is_success = false;
      }

      const auto end_time = std::chrono::steady_clock::now();

      const size_t duration = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();
      total_duration += duration;

      PrintConsoleInfo(is_success ? ConsoleInfoType::kPassed : ConsoleInfoType::kFailed,
                       benchmark_names[i] + " (" + std::to_string(duration) + " s)");
      PrintConsoleInfo(ConsoleInfoType::kSingleSeparator);
    }

    PrintConsoleInfo(ConsoleInfoType::kDoubleSeparator, std::to_string(benchmark_names.size()) + " benchmark" +
                                                            (benchmark_names.size() > 1 ? "s" : "") + " ran (" +
                                                            std::to_string(total_duration) + " s total)");

    // Generate the output
    const auto output =
        Aws::Utils::Json::JsonValue()
            .WithObject("context", Aws::Utils::Json::JsonValue()
                                       .WithString("date", skyrise::GetFormattedTimestamp("%Y/%m/%d-%H:%M:%S"))
                                       .WithString("commit", GitMetadata::CommitSha1()))
            .WithArray("benchmarks", skyrise::FlattenArrays(&benchmark_results));

    // Save the output
    skyrise::WriteStringToFile(output.View().WriteReadable(), cli_arguments["output"].as<std::string>());

  } catch (const std::exception& exception) {
    std::cout << exception.what() << "\n";

    return_code = 1;
  }

  Aws::ShutdownAPI(sdk_options);

  return return_code;
}
