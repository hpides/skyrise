#include "benchmark_helper.hpp"

#include <algorithm>
#include <cmath>
#include <iterator>
#include <numeric>

namespace skyrise {

BenchmarkAggregates BenchmarkHelper::CalculateAggregates(
    const std::shared_ptr<std::vector<BenchmarkItemResult>>& benchmark_result,
    const std::function<double(const BenchmarkItemResult&)>& extract_metric) {
  std::vector<double> metrics;
  std::transform(benchmark_result->cbegin(), benchmark_result->cend(), std::back_inserter(metrics),
                 [&](const BenchmarkItemResult& result) { return extract_metric(result); });

  return CalculateAggregates(metrics);
}

BenchmarkAggregates BenchmarkHelper::CalculateAggregates(std::vector<double>& metrics) {
  if (metrics.empty()) {
    return {};
  }

  std::sort(metrics.begin(), metrics.end());

  const double minimum = metrics.front();
  const double maximum = metrics.back();
  const double average = std::accumulate(metrics.cbegin(), metrics.cend(), 0.0) / metrics.size();

  const double median = metrics.size() % 2 == 0 ? (metrics[metrics.size() / 2 - 1] + metrics[metrics.size() / 2]) / 2
                                                : metrics[(metrics.size() / 2)];
  const double percentile_90 = metrics[static_cast<size_t>(metrics.size() * 0.9)];
  const double percentile_99 = metrics[static_cast<size_t>(metrics.size() * 0.99)];
  const double percentile_99_9 = metrics[static_cast<size_t>(metrics.size() * 0.999)];
  const double percentile_99_99 = metrics[static_cast<size_t>(metrics.size() * 0.9999)];

  const double variance = std::accumulate(metrics.cbegin(), metrics.cend(), 0.0,
                                          [&](double a, double b) { return a + std::pow(b - average, 2); }) /
                          static_cast<double>(metrics.size());
  const double standard_deviation = std::sqrt(variance);

  return {minimum,         maximum,          average,           median, percentile_90, percentile_99,
          percentile_99_9, percentile_99_99, standard_deviation};
}

Aws::Utils::Json::JsonValue BenchmarkHelper::GenerateJsonOutput(
    const Aws::String& benchmark_name, const std::vector<std::tuple<Aws::String, double>>& aggregated_metrics,
    const std::shared_ptr<std::vector<BenchmarkItemResult>>& benchmark_result,
    const std::vector<std::function<std::tuple<Aws::String, double>(const BenchmarkItemResult&)>>& metrics) {
  auto json_output = Aws::Utils::Json::JsonValue().WithString("name", benchmark_name);

  for (const auto& [metric_name, metric] : aggregated_metrics) {
    json_output = json_output.WithDouble(metric_name, metric);
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> benchmark_runs(benchmark_result->size());

  for (size_t i = 0; i < benchmark_result->size(); i++) {
    auto benchmark_run_value =
        Aws::Utils::Json::JsonValue().WithString("name", benchmark_name + "/" + std::to_string(i));

    for (const auto& extract_metric : metrics) {
      const auto& [metric_name, metric] = extract_metric(benchmark_result->at(i));
      benchmark_run_value = benchmark_run_value.WithDouble(metric_name, metric);
    }

    benchmark_runs[i] = benchmark_run_value;
  }

  json_output = json_output.WithArray("runs", benchmark_runs);

  return json_output;
}

}  // namespace skyrise
