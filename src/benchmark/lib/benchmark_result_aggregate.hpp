#pragma once

#include <cstddef>
#include <vector>

namespace skyrise {

// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions,hicpp-special-member-functions)
class BenchmarkResultAggregate {
 public:
  explicit BenchmarkResultAggregate(std::vector<double> values);
  BenchmarkResultAggregate(std::vector<double> values, size_t decimal_precision);
  BenchmarkResultAggregate(const BenchmarkResultAggregate&) = delete;
  BenchmarkResultAggregate& operator=(const BenchmarkResultAggregate&) = delete;

  // The median and the other percentiles are calculated according to the nearest-rank, exclusive definition
  // (cf. https://en.wikipedia.org/wiki/Percentile).
  double GetAverage() const;
  double GetMaximum() const;
  double GetMedian() const;
  double GetMinimum() const;
  double GetPercentile(double percentile) const;
  double GetStandardDeviation() const;
  double GetVariance() const;

 private:
  double Round(double value) const;

  std::vector<double> values_;
  size_t decimal_precision_ = 3;
};

}  // namespace skyrise
