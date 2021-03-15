#include "benchmark_executable.hpp"

#include <cstdlib>
#include <iostream>

#include <aws/core/Aws.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/LogLevel.h>

#include "utils/filesystem.hpp"
#include "utils/git_metadata.hpp"
#include "utils/time.hpp"

BenchmarkExecutable::BenchmarkExecutable(const std::string& executable_name, const std::string& benchmark_name)
    : cli_options_(executable_name, benchmark_name), cli_option_adder_(cli_options_.add_options()) {
  cli_option_adder_("output", "The output file <file.json>", cxxopts::value<std::string>());

  sdk_options_.httpOptions.installSigPipeHandler = true;
}

cxxopts::OptionAdder& BenchmarkExecutable::GetOptionAdder() { return cli_option_adder_; }

cxxopts::ParseResult& BenchmarkExecutable::GetParseResult(
    int argc, char* argv[]) {  // NOLINT(cppcoreguidelines-avoid-c-arrays,hicpp-avoid-c-arrays,modernize-avoid-c-arrays)
  cli_option_adder_("verbose", "Show the verbose status log", cxxopts::value<bool>());
  cli_option_adder_("help", "Print the usage overview", cxxopts::value<bool>());

  cli_options_.parse_positional({"output"});
  cli_options_.positional_help("OUTPUT");

  cli_parse_result_ = cli_options_.parse(argc, argv);

  if (cli_parse_result_.count("help") > 0) {
    std::cout << cli_options_.help();

    std::exit(0);
  }

  if (cli_parse_result_.count("output") == 0) {
    throw cxxopts::option_required_exception("OUTPUT");
  }

  if (cli_parse_result_.count("verbose") > 0) {
    Aws::Utils::Logging::LogLevel log_level{Aws::Utils::Logging::LogLevel::Info};
    sdk_options_.loggingOptions.logLevel = log_level;
    sdk_options_.loggingOptions.logger_create_fn = [log_level]() {
      return Aws::MakeShared<Aws::Utils::Logging::ConsoleLogSystem>("console_logger", log_level);
    };
  }

  InitializeClients();

  return cli_parse_result_;
}

std::shared_ptr<skyrise::Client> BenchmarkExecutable::GetClient() const { return client_; }

std::shared_ptr<skyrise::CostCalculator> BenchmarkExecutable::GetCostCalculator() const { return cost_calculator_; }

std::shared_ptr<skyrise::BenchmarkRunner> BenchmarkExecutable::GetBenchmarkRunner() const { return benchmark_runner_; }

std::shared_ptr<skyrise::BenchmarkHelper> BenchmarkExecutable::GetBenchmarkHelper() const { return benchmark_helper_; }

void BenchmarkExecutable::ExecuteBenchmark(const std::shared_ptr<skyrise::Benchmark>& benchmark) {
  const auto benchmark_result = benchmark->Run(benchmark_runner_);

  const auto output =
      Aws::Utils::Json::JsonValue()
          .WithObject("context", Aws::Utils::Json::JsonValue()
                                     .WithString("date", skyrise::GetFormattedTimestamp("%Y/%m/%d-%H:%M:%S"))
                                     .WithString("commit", GitMetadata::CommitSha1()))
          .WithArray("benchmarks", benchmark_result);

  skyrise::WriteStringToFile(output.View().WriteReadable(), cli_parse_result_["output"].as<std::string>());

  DeinitializeClients();
}

void BenchmarkExecutable::InitializeClients() {
  Aws::InitAPI(sdk_options_);

  client_ = std::make_shared<skyrise::Client>();
  cost_calculator_ = std::make_shared<skyrise::CostCalculator>(client_);
  benchmark_runner_ = std::make_shared<skyrise::BenchmarkRunner>(client_);
}

void BenchmarkExecutable::DeinitializeClients() { Aws::ShutdownAPI(sdk_options_); }
