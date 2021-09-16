#pragma once

#include <aws/core/Aws.h>
#include <gtest/gtest_prod.h>

#include "abstract_benchmark.hpp"
#include "abstract_benchmark_runner.hpp"
#include "lambda_benchmark_result.hpp"
#include "lambda_benchmark_runner.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

class LambdaBenchmark : public AbstractBenchmark {
 public:
  LambdaBenchmark(std::shared_ptr<const CostCalculator> cost_calculator);

  LambdaBenchmark(const LambdaBenchmark&) = delete;
  LambdaBenchmark& operator=(const LambdaBenchmark&) = delete;

  virtual ~LambdaBenchmark() = default;

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(
      const std::shared_ptr<AbstractBenchmarkRunner>& benchmark_runner) override;

 protected:
  virtual Aws::Utils::Array<Aws::Utils::Json::JsonValue> OnRun(
      const std::shared_ptr<LambdaBenchmarkRunner>& benchmark_runner) = 0;
  long double CalculateOverallFunctionCost(const std::shared_ptr<LambdaBenchmarkResult>& benchmark_result,
                                           const size_t function_instance_mb_size,
                                           const bool is_provisioned_concurrency = false) const;
  long double ExtractFunctionCost(const LambdaInvokeResult& invoke_result, const size_t function_instance_mb_size,
                                  const bool is_provisioned_concurrency = false) const;

  // TODO(maltenbergert): Split up GenerateJsonOutput
  FRIEND_TEST(AwsBenchmarkTest, GenerateJsonOutput);
  static Aws::Utils::Json::JsonValue GenerateJsonOutput(
      const Aws::String& benchmark_name, const std::vector<std::tuple<Aws::String, double>>& aggregated_numeric_metrics,
      const std::vector<std::tuple<Aws::String, Aws::String>>& aggregated_alphabetic_metrics,
      const std::shared_ptr<LambdaBenchmarkResult>& benchmark_result,
      const std::vector<std::function<std::tuple<Aws::String, double>(const LambdaInvokeResult&)>>&
          extract_numeric_metric_functions,
      const std::vector<std::function<std::tuple<Aws::String, Aws::String>(const LambdaInvokeResult&)>>&
          extract_alphabetic_metric_functions,
      const std::vector<std::function<std::tuple<Aws::String, Aws::Utils::Json::JsonValue>(const LambdaInvokeResult&)>>&
          extract_object_metric_functions);

  const std::shared_ptr<const CostCalculator> cost_calculator_;
};

}  // namespace skyrise
