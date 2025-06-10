#include "lambda_benchmark.hpp"

#include <numeric>

#include "utils/assert.hpp"
#include "utils/string.hpp"

namespace skyrise {

LambdaBenchmark::LambdaBenchmark(std::shared_ptr<const CostCalculator> cost_calculator)
    : cost_calculator_(std::move(cost_calculator)) {}

Aws::Utils::Array<Aws::Utils::Json::JsonValue> LambdaBenchmark::Run(
    const std::shared_ptr<AbstractBenchmarkRunner>& benchmark_runner) {
  const auto lambda_benchmark_runner = std::dynamic_pointer_cast<LambdaBenchmarkRunner>(benchmark_runner);
  Assert(lambda_benchmark_runner, "LambdaBenchmark needs a LambdaBenchmarkRunner to run.");
  return OnRun(lambda_benchmark_runner);
}

}  // namespace skyrise
