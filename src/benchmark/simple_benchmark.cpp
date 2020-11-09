#include <iomanip>
#include <iostream>

#include <aws/core/Aws.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <magic_enum.hpp>

#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"
#include "client/client_aws.hpp"

// This hacky place is a playground to try out the BenchmarkRunner.

const size_t kLambdaSize = 128;
const size_t kNumInvocations = 10;
const size_t kNumRepetitions = 2;
const std::vector<std::function<void()>> kAfterRepetitonCallbacks{[]() { std::cout << "After Repetition 0"; },
                                                                  []() { std::cout << "After Repetition 1"; }};

int main() {
  Aws::SDKOptions options;
  options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
  options.loggingOptions.logger_create_fn = [] {
    return std::make_shared<Aws::Utils::Logging::ConsoleLogSystem>(Aws::Utils::Logging::LogLevel::Info);
  };

  Aws::InitAPI(options);
  // TODO(anyone): Refactor this block
  {
    std::cout << "Creating Clients...\n";

    const auto clients = std::make_shared<skyrise::ClientAws>();

    std::cout << "Clients created.\n\n";

    std::cout << "Creating BenchmarkConfigs...\n";

    std::vector<skyrise::BenchmarkConfig> configs{
        {"skyriseFunctionMinimal", kLambdaSize, kNumInvocations, skyrise::ExecuteMode::WarmAsync, kNumRepetitions,
         kAfterRepetitonCallbacks},
        {"skyriseFunctionMinimal", kLambdaSize, kNumInvocations, skyrise::ExecuteMode::WarmParallel, kNumRepetitions,
         kAfterRepetitonCallbacks},
        {"skyriseFunctionMinimal", kLambdaSize, kNumInvocations, skyrise::ExecuteMode::WarmSequential, kNumRepetitions,
         kAfterRepetitonCallbacks},
    };
    std::cout << "BenchmarkConfigs created.\n\n";

    std::cout << "Creating BenchmarkRunner...\n";

    skyrise::BenchmarkRunner runner(clients);

    std::cout << "BenchmarkRunner created.\n\n";

    for (const auto& config : configs) {
      const auto results = runner.RunConfig(config);

      std::cout << "*****************************************************************************************\n";
      std::cout << "Execute Mode: " << magic_enum::enum_name(config.execute_mode_) << "\n";
      std::cout << "*****************************************************************************************\n";
      std::cout << "\nID\t\t\t\t\t\t\t| Success\t| Duration [ms]\n";
      std::cout << "--------------------------------------------------------|---------------|----------------\n";
      for (const auto& result : *results) {
        std::cout << result.invocation_id << "\t| ";
        std::cout << std::boolalpha << result.success << "\t\t| ";
        std::cout << std::fixed << std::setprecision(3)
                  << std::chrono::duration<double, std::milli>(result.end_time - result.start_time).count() << "\n";
      }
      std::cout << "*****************************************************************************************\n\n";
    }
  }
  Aws::ShutdownAPI(options);

  return 0;
}
