#include "benchmark_result_aggregate.hpp"

#include <algorithm>
#include <cmath>
#include <numeric>
#include <string>

namespace skyrise {

BenchmarkResultAggregate::BenchmarkResultAggregate(std::vector<double> values) : values_(std::move(values)) {
  std::sort(values_.begin(), values_.end());
}

double BenchmarkResultAggregate::GetAverage() const {
  return std::accumulate(values_.cbegin(), values_.cend(), 0.0) / values_.size();
}

double BenchmarkResultAggregate::GetMaximum() const { return values_.back(); }

double BenchmarkResultAggregate::GetMedian() const { return GetPercentile(50); }

double BenchmarkResultAggregate::GetMinimum() const { return values_.front(); }

double BenchmarkResultAggregate::GetPercentile(double percentile) const {
  return values_[static_cast<size_t>(values_.size() * percentile / 100)];
}

double BenchmarkResultAggregate::GetStandardDeviation() const { return std::sqrt(GetVariance()); }

double BenchmarkResultAggregate::GetVariance() const {
  const double average = GetAverage();

  return std::accumulate(values_.cbegin(), values_.cend(), 0.0,
                         [&](double a, double b) { return a + std::pow(b - average, 2); }) /
         static_cast<double>(values_.size());
}

}  // namespace skyrise
