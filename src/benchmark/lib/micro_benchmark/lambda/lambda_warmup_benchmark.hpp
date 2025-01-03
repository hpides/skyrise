#pragma once

#include <memory>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "lambda_benchmark.hpp"
#include "lambda_benchmark_config.hpp"
#include "lambda_benchmark_runner.hpp"
#include "lambda_benchmark_types.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

class LambdaWarmupBenchmark : public LambdaBenchmark {
 public:
  LambdaWarmupBenchmark(std::shared_ptr<const CostCalculator> cost_calculator,
                        const std::vector<size_t>& function_instance_sizes_mb,
                        const std::vector<size_t>& concurrent_instance_counts, const size_t repetition_count,
                        const bool enable_provisioned_concurrency, const std::vector<double>& provisioning_factors);
  const Aws::String& Name() const override;

 protected:
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> OnRun(const std::shared_ptr<LambdaBenchmarkRunner>& runner) override;

 private:
  static bool IsWarmFunction(const LambdaInvocationResult& invocation_result, const std::string& warmup_strategy);

  Aws::Utils::Json::JsonValue GenerateResultOutput(const std::shared_ptr<LambdaBenchmarkResult>& result,
                                                   const LambdaWarmupBenchmarkParameters& parameters) const;

  std::vector<std::pair<LambdaWarmupBenchmarkParameters, std::shared_ptr<LambdaBenchmarkConfig>>> configs_;
};

}  // namespace skyrise
