#pragma once

#include <memory>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "lambda_benchmark.hpp"
#include "lambda_benchmark_config.hpp"
#include "lambda_benchmark_runner.hpp"
#include "lambda_benchmark_types.hpp"
#include "micro_benchmark/micro_benchmark_utils.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

class LambdaStartupThroughputBenchmark : public LambdaBenchmark {
 public:
  LambdaStartupThroughputBenchmark(std::shared_ptr<const CostCalculator> cost_calculator,
                                   const std::vector<size_t>& function_instance_sizes_mb,
                                   const std::vector<size_t>& concurrent_instance_counts, const size_t repetition_count,
                                   const std::vector<size_t>& payload_byte_sizes);
  const Aws::String& Name() const override;

 protected:
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> OnRun(const std::shared_ptr<LambdaBenchmarkRunner>& runner) override;

 private:
  Aws::Utils::Json::JsonValue GenerateResultOutput(const std::shared_ptr<LambdaBenchmarkResult>& result,
                                                   const LambdaStartupBenchmarkParameters& parameters) const;

  std::vector<std::pair<LambdaStartupBenchmarkParameters, std::shared_ptr<LambdaBenchmarkConfig>>> benchmark_configs_;
};

}  // namespace skyrise
