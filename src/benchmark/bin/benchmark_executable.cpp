#include "benchmark_executable.hpp"

#include <cstdlib>
#include <iostream>

#include <aws/core/Aws.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/LogLevel.h>

#include "client/coordinator_client.hpp"
#include "micro_benchmark/ec2/ec2_startup_benchmark.hpp"
#include "micro_benchmark/ec2/ec2_storage_benchmark.hpp"
#include "micro_benchmark/lambda/lambda_benchmark.hpp"
#include "system_benchmark/system_benchmark.hpp"
#include "utils/assert.hpp"
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
  cli_option_adder_("disable_metering", "Disable metering.", cxxopts::value<bool>());
  cli_option_adder_("verbose", "Show the verbose status log", cxxopts::value<bool>());
  cli_option_adder_("help", "Print the usage overview", cxxopts::value<bool>());

  cli_options_.parse_positional({"output"});
  cli_options_.positional_help("OUTPUT");

  cli_parse_result_ = cli_options_.parse(argc, argv);

  if (cli_parse_result_.count("help") > 0) {
    std::cout << cli_options_.help();

    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    std::exit(0);
  }

  if (cli_parse_result_.count("output") == 0) {
    throw cxxopts::exceptions::requested_option_not_present("OUTPUT");
  }

  if (cli_parse_result_.count("disable_metering") == 0) {
    request_tracker_ = std::make_shared<skyrise::RequestTracker>();
    request_tracker_->Install(&sdk_options_);
    enable_metering_ = true;
  }

  if (cli_parse_result_.count("verbose") > 0) {
    const Aws::Utils::Logging::LogLevel log_level{Aws::Utils::Logging::LogLevel::Info};
    const Aws::Utils::Logging::LogLevel crt_log_level{Aws::Utils::Logging::LogLevel::Warn};
    sdk_options_.loggingOptions.logLevel = log_level;
    sdk_options_.loggingOptions.logger_create_fn = [log_level]() {
      return Aws::MakeShared<Aws::Utils::Logging::ConsoleLogSystem>("console_logger", log_level);
    };
    sdk_options_.loggingOptions.crt_logger_create_fn = [crt_log_level]() {
      return Aws::MakeShared<Aws::Utils::Logging::DefaultCRTLogSystem>("default_crt_logger", crt_log_level);
    };
  }

  InitializeClients();

  return cli_parse_result_;
}

const skyrise::CoordinatorClient& BenchmarkExecutable::GetClient() const { return *client_; }

std::shared_ptr<const skyrise::CostCalculator> BenchmarkExecutable::GetCostCalculator() const {
  return cost_calculator_;
}

std::shared_ptr<skyrise::Ec2BenchmarkRunner> BenchmarkExecutable::GetEc2BenchmarkRunner() const {
  return ec2_benchmark_runner_;
}

std::shared_ptr<skyrise::LambdaBenchmarkRunner> BenchmarkExecutable::GetLambdaBenchmarkRunner() const {
  return lambda_benchmark_runner_;
}

void BenchmarkExecutable::ExecuteBenchmark(const std::shared_ptr<skyrise::AbstractBenchmark>& benchmark) {
  const auto benchmark_result = [&]() {
    if (std::dynamic_pointer_cast<skyrise::Ec2StartupBenchmark>(benchmark) ||
        std::dynamic_pointer_cast<skyrise::Ec2StorageBenchmark>(benchmark)) {
      return benchmark->Run(ec2_benchmark_runner_);
      // NOLINTNEXTLINE(bugprone-branch-clone)
    } else if (std::dynamic_pointer_cast<skyrise::LambdaBenchmark>(benchmark)) {
      return benchmark->Run(lambda_benchmark_runner_);
      // NOLINTNEXTLINE(bugprone-branch-clone)
    } else if (std::dynamic_pointer_cast<skyrise::SystemBenchmark>(benchmark)) {
      return benchmark->Run(system_benchmark_runner_);
      // NOLINTNEXTLINE(bugprone-branch-clone)
    } else if (std::dynamic_pointer_cast<skyrise::AbstractBenchmark>(benchmark)) {
      return benchmark->Run(lambda_benchmark_runner_);
    } else {
      Fail("Unknown benchmark type.");
    }
  }();

  const auto output =
      Aws::Utils::Json::JsonValue()
          .WithObject("context", Aws::Utils::Json::JsonValue()
                                     .WithString("name", benchmark->Name())
                                     .WithString("commit", GitMetadata::CommitSha1())
                                     .WithString("date", skyrise::GetFormattedTimestamp("%Y/%m/%d-%H:%M:%S")))
          .WithArray("runs", benchmark_result);

  skyrise::WriteStringToFile(output.View().WriteReadable(), cli_parse_result_["output"].as<std::string>());

  DeinitializeClients();

  PrintRequestSummary();
}

void BenchmarkExecutable::InitializeClients() {
  Aws::InitAPI(sdk_options_);

  client_ = std::make_shared<skyrise::CoordinatorClient>();

  cost_calculator_ =
      std::make_shared<const skyrise::CostCalculator>(client_->GetPricingClient(), client_->GetClientRegion());

  ec2_benchmark_runner_ =
      std::make_shared<skyrise::Ec2BenchmarkRunner>(client_->GetEc2Client(), client_->GetSsmClient(), enable_metering_);

  lambda_benchmark_runner_ = std::make_shared<skyrise::LambdaBenchmarkRunner>(
      client_->GetIamClient(), client_->GetLambdaClient(), client_->GetSqsClient(), cost_calculator_,
      client_->GetClientRegion(), enable_metering_);

  system_benchmark_runner_ = std::make_shared<skyrise::SystemBenchmarkRunner>(
      client_->GetIamClient(), client_->GetLambdaClient(), client_->GetS3Client(), cost_calculator_,
      client_->GetClientRegion(), enable_metering_);
}

void BenchmarkExecutable::DeinitializeClients() { Aws::ShutdownAPI(sdk_options_); }

void BenchmarkExecutable::PrintRequestSummary() {
  if (request_tracker_) {
    request_tracker_->WriteSummaryToStream(&std::cout);
  }
}
