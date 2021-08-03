#pragma once

#include <aws/core/Aws.h>
#include <gtest/gtest_prod.h>

#include "benchmark_result.hpp"
#include "benchmark_runner.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

class Benchmark {
 public:
  Benchmark(std::shared_ptr<const CostCalculator> cost_calculator);

  Benchmark(const Benchmark&) = delete;
  Benchmark& operator=(const Benchmark&) = delete;

  virtual ~Benchmark() = default;

  virtual Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(
      const std::shared_ptr<BenchmarkRunner>& benchmark_runner) = 0;

 protected:
  long double CalculateOverallFunctionCost(const std::shared_ptr<BenchmarkResult>& benchmark_result,
                                           const size_t function_instance_mb_size,
                                           const bool is_provisioned_concurrency = false) const;
  long double ExtractFunctionCost(const InvokeResult& invoke_result, const size_t function_instance_mb_size,
                                  const bool is_provisioned_concurrency = false) const;

  // TODO(maltenbergert): Split up GenerateJsonOutput
  FRIEND_TEST(AwsBenchmarkTest, GenerateJsonOutput);
  static Aws::Utils::Json::JsonValue GenerateJsonOutput(
      const Aws::String& benchmark_name, const std::vector<std::tuple<Aws::String, double>>& aggregated_numeric_metrics,
      const std::vector<std::tuple<Aws::String, Aws::String>>& aggregated_alphabetic_metrics,
      const std::shared_ptr<BenchmarkResult>& benchmark_result,
      const std::vector<std::function<std::tuple<Aws::String, double>(const InvokeResult&)>>&
          extract_numeric_metric_functions,
      const std::vector<std::function<std::tuple<Aws::String, Aws::String>(const InvokeResult&)>>&
          extract_alphabetic_metric_functions,
      const std::vector<std::function<std::tuple<Aws::String, Aws::Utils::Json::JsonValue>(const InvokeResult&)>>&
          extract_object_metric_functions);

  const std::shared_ptr<const CostCalculator> cost_calculator_;
};

}  // namespace skyrise
