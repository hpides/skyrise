#pragma once

#include <vector>

namespace skyrise {

class BenchmarkResultAggregate {
 public:
  BenchmarkResultAggregate(std::vector<double> values);
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
  std::vector<double> values_;
};

}  // namespace skyrise
