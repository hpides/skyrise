#include "benchmark_result_aggregate.hpp"

#include <cmath>
#include <numeric>
#include <vector>

#include <gtest/gtest.h>

namespace skyrise {

class BenchmarkResultAggregateTest : public ::testing::Test {};

TEST_F(BenchmarkResultAggregateTest, CalculateAggregates) {
  static constexpr size_t kValueCount = 1000;

  std::vector<double> values(kValueCount);
  std::iota(values.begin(), values.end(), 0);

  const double average = (kValueCount - 1) / 2.0;

  const BenchmarkResultAggregate aggregates(values, 15);

  double variance = 0;
  for (size_t i = 0; i < kValueCount; ++i) {
    variance += std::pow(i - average, 2);
  }

  variance /= kValueCount;
  const double std_dev = std::sqrt(variance);

  EXPECT_DOUBLE_EQ(aggregates.GetMinimum(), 0.0);
  EXPECT_DOUBLE_EQ(aggregates.GetMaximum(), kValueCount - 1);
  EXPECT_DOUBLE_EQ(aggregates.GetAverage(), average);
  EXPECT_DOUBLE_EQ(aggregates.GetMedian(), kValueCount * 0.5);
  EXPECT_DOUBLE_EQ(aggregates.GetPercentile(0.1), kValueCount * 0.001);
  EXPECT_DOUBLE_EQ(aggregates.GetPercentile(1), kValueCount * 0.01);
  EXPECT_DOUBLE_EQ(aggregates.GetPercentile(90), kValueCount * 0.9);
  EXPECT_DOUBLE_EQ(aggregates.GetPercentile(99.9), kValueCount * 0.999);
  EXPECT_DOUBLE_EQ(aggregates.GetStandardDeviation(), std_dev);
}

}  // namespace skyrise
