#include <iostream>
#include <memory>

#include <aws/core/Aws.h>

#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"

// This hacky place is a playground to try out the BenchmarkRunner.

const size_t kLambdaSize = 128;
const size_t kNumInvocations = 15;

int main() {
  Aws::SDKOptions options;
  Aws::InitAPI(options);
  // TODO: Refactor this block
  {
    std::cout << "Creating BenchmarkConfigs...\n";

    std::vector<skyrise::BenchmarkConfig> configs{
        {"skyriseFunctionMinimal", kLambdaSize, "AWSLambda", kNumInvocations, skyrise::ExecuteMode::WarmAsync},
        {"skyriseFunctionMinimal", kLambdaSize, "AWSLambda", kNumInvocations, skyrise::ExecuteMode::WarmParallel},
        {"skyriseFunctionMinimal", kLambdaSize, "AWSLambda", kNumInvocations, skyrise::ExecuteMode::WarmSequential}};

    std::cout << "BenchmarkConfigs created.\n";

    std::cout << "Creating BenchmarkRunner...\n";

    skyrise::BenchmarkRunner runner;

    std::cout << "BenchmarkRunner created.\n";

    for (const auto& config : configs) {
      runner.RunConfig(config);

      const auto results = runner.GetBenchmarkResult();

      std::cout << "\n*****************************************************************************************\n";
      // TODO: Add skyrise::ExecuteMode to std::string translation needed in the JSON output as well
      std::cout << "Execute Mode: " << static_cast<int>(config.execute_mode_) << "\n\n";
      std::cout << "\nID\t\t\t\t\t\t\t| Success\t| Duration ms\n";
      std::cout << "--------------------------------------------------------|---------------|----------------\n";
      for (const auto& result : *results) {
        std::cout << result.invocation_id << "\t|\t";
        std::cout << result.success << "\t| ";
        std::cout << (result.end_time - result.start_time).count() / 1'000'000.0 << "\n";
      }
      std::cout << "*****************************************************************************************\n";
    }
  }
  Aws::ShutdownAPI(options);

  return 0;
}
