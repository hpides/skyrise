#include "benchmark_result_aggregate.hpp"

#include <algorithm>
#include <cmath>
#include <numeric>

namespace skyrise {

BenchmarkResultAggregate::BenchmarkResultAggregate(std::vector<double> values) : values_(std::move(values)) {
  std::sort(values_.begin(), values_.end());
}

BenchmarkResultAggregate::BenchmarkResultAggregate(std::vector<double> values, size_t decimal_precision)
    : BenchmarkResultAggregate(std::move(values)) {
  // NOLINTNEXTLINE(cppcoreguidelines-prefer-member-initializer)
  decimal_precision_ = decimal_precision;
}

double BenchmarkResultAggregate::GetAverage() const {
  return Round(std::accumulate(values_.cbegin(), values_.cend(), 0.0) / values_.size());
}

double BenchmarkResultAggregate::GetMaximum() const { return Round(values_.back()); }

double BenchmarkResultAggregate::GetMedian() const { return Round(GetPercentile(50)); }

double BenchmarkResultAggregate::GetMinimum() const { return Round(values_.front()); }

double BenchmarkResultAggregate::GetPercentile(double percentile) const {
  return Round(values_[static_cast<size_t>(values_.size() * percentile / 100)]);
}

double BenchmarkResultAggregate::GetStandardDeviation() const { return Round(std::sqrt(GetVariance())); }

double BenchmarkResultAggregate::GetVariance() const {
  const double average = GetAverage();
  const double variance = std::accumulate(values_.cbegin(), values_.cend(), 0.0,
                                          [&](double a, double b) { return a + std::pow(b - average, 2); }) /
                          static_cast<double>(values_.size());
  return Round(variance);
}

double BenchmarkResultAggregate::Round(double value) const {
  const double multiplier = std::pow(10, decimal_precision_);
  return std::round(value * multiplier) / multiplier;
}

}  // namespace skyrise
