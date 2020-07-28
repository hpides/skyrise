#include <iostream>
#include <memory>

#include <aws/core/Aws.h>

#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"

// This hacky place is a playground to try out the BenchmarkRunner.

const size_t kLambdaSize = 128;
const size_t kNumInvocations = 15;
const skyrise::ExecuteMode kExecuteMode = skyrise::ExecuteMode::WarmAsync;

int main() {
  Aws::SDKOptions options;
  Aws::InitAPI(options);
  // TODO: Refactor this block
  {
    std::cout << "Creating config...\n";

    const skyrise::BenchmarkConfig config("skyriseFunctionMinimal", kLambdaSize, "AWSLambda", kNumInvocations,
                                          kExecuteMode);

    std::cout << "Config created.\n";

    std::cout << "Creating BenchmarkRunner...\n";
    skyrise::BenchmarkRunner benchmark_runner(config);
    std::cout << "BenchmarkRunner created.\n";

    std::cout << "\n########################\n\n";
    std::cout << "#Invocations: " << kNumInvocations << "\n";
    std::cout << "Execute Mode: Warm Async\n";
    std::cout << "\n########################\n\n";

    benchmark_runner.Run();

    const auto results = benchmark_runner.GetBenchmarkResult();
    std::cout << "\nID\t\t\t\t\t\t\t| Success\t| Duration ms\n";
    std::cout << "--------------------------------------------------------|---------------|----------------\n";
    for (const auto& result : *results) {
      std::cout << result.invocation_id << "\t|\t";
      std::cout << result.success << "\t| ";
      std::cout << (result.end_time - result.start_time).count() / 1'000'000.0 << "\n";
    }
  }
  Aws::ShutdownAPI(options);

  return 0;
}
