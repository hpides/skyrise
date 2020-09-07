#pragma once

#include <functional>
#include <memory>
#include <tuple>

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "benchmark_runner.hpp"

namespace skyrise {

struct BenchmarkAggregates {
  double minimum;
  double maximum;
  double average;
  double median;
  double percentile_90;
  double percentile_99;
  double percentile_99_9;
  double percentile_99_99;
  double standard_deviation;
};

class BenchmarkHelper {
 public:
  BenchmarkHelper() {}
  BenchmarkAggregates CalculateAggregates(std::shared_ptr<std::vector<BenchmarkItemResult>> benchmark_result,
                                          std::function<double(const BenchmarkItemResult&)> extract_metric);
  BenchmarkAggregates CalculateAggregates(std::vector<double>& metrics);
  Aws::Utils::Json::JsonValue GenerateJsonOutput(
      const Aws::String& benchmark_name, const std::vector<std::tuple<Aws::String, double>>& aggregated_metrics,
      std::shared_ptr<std::vector<BenchmarkItemResult>> benchmark_result,
      const std::vector<std::function<std::tuple<Aws::String, double>(const BenchmarkItemResult&)>>& metrics);
};

}  // namespace skyrise
