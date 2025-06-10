#pragma once

#include <aws/core/Aws.h>

#include "abstract_benchmark.hpp"
#include "abstract_benchmark_runner.hpp"
#include "lambda_benchmark_result.hpp"
#include "lambda_benchmark_runner.hpp"
#include "lambda_benchmark_types.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions,hicpp-special-member-functions)
class LambdaBenchmark : public AbstractBenchmark {
 public:
  explicit LambdaBenchmark(std::shared_ptr<const CostCalculator> cost_calculator);

  LambdaBenchmark(const LambdaBenchmark&) = delete;
  LambdaBenchmark& operator=(const LambdaBenchmark&) = delete;

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(
      const std::shared_ptr<AbstractBenchmarkRunner>& benchmark_runner) override;

 protected:
  virtual Aws::Utils::Array<Aws::Utils::Json::JsonValue> OnRun(
      const std::shared_ptr<LambdaBenchmarkRunner>& benchmark_runner) = 0;

  const std::shared_ptr<const CostCalculator> cost_calculator_;
};

}  // namespace skyrise
