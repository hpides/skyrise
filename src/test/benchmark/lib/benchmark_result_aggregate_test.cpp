#include "benchmark_result_aggregate.hpp"

#include <cmath>
#include <numeric>
#include <vector>

#include "gtest/gtest.h"

namespace skyrise {

class BenchmarkResultAggregateTest : public ::testing::Test {};

TEST_F(BenchmarkResultAggregateTest, CalculateAggregates) {
  const size_t value_count = 1000;

  std::vector<double> values(value_count);
  std::iota(values.begin(), values.end(), 0);

  const double average = (value_count - 1) / 2.0;

  const BenchmarkResultAggregate aggregates(values);

  double variance = 0;
  for (size_t i = 0; i < value_count; i++) {
    variance += std::pow(i - average, 2);
  }

  variance /= value_count;
  const double std_dev = std::sqrt(variance);

  EXPECT_EQ(aggregates.GetMinimum(), 0.0);
  EXPECT_EQ(aggregates.GetMaximum(), value_count - 1);
  EXPECT_EQ(aggregates.GetAverage(), average);
  EXPECT_EQ(aggregates.GetMedian(), value_count / 2);
  EXPECT_EQ(aggregates.GetPercentile(0.1), value_count * 0.001);
  EXPECT_EQ(aggregates.GetPercentile(1), value_count * 0.01);
  EXPECT_EQ(aggregates.GetPercentile(90), value_count * 0.9);
  EXPECT_EQ(aggregates.GetPercentile(99.9), value_count * 0.999);
  EXPECT_EQ(aggregates.GetStandardDeviation(), std_dev);
}

}  // namespace skyrise
