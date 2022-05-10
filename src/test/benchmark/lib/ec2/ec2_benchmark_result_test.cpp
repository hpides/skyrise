#include "ec2/ec2_benchmark_result.hpp"

#include <gtest/gtest.h>

namespace skyrise {

TEST(Ec2BenchmarkResultTest, BasicFunctionality) {
  const size_t repetition_count = 10;
  const size_t invocation_count = 20;

  Ec2BenchmarkResult benchmark_result(repetition_count, invocation_count);

  for (size_t i = 0; i < repetition_count; ++i) {
    for (size_t j = 0; j < invocation_count; ++j) {
      EXPECT_FALSE(benchmark_result.IsResultComplete());
      EXPECT_FALSE(benchmark_result.IsRepetitionComplete(i));
      benchmark_result.RegisterInstanceLaunch(i, std::to_string(j), 1.0);
      benchmark_result.UpdateCooldown(i, std::to_string(j), 2.0);
    }

    EXPECT_TRUE(benchmark_result.IsRepetitionComplete(i));
    EXPECT_FALSE(benchmark_result.IsRepetitionFinalized(i));
    benchmark_result.FinalizeRepetition(i, 1.0);
    EXPECT_TRUE(benchmark_result.IsRepetitionFinalized(i));
  }

  EXPECT_TRUE(benchmark_result.IsResultComplete());
  EXPECT_FALSE(benchmark_result.IsResultFinalized());
  benchmark_result.FinalizeResult(10.0);
  EXPECT_TRUE(benchmark_result.IsResultFinalized());
  EXPECT_EQ(benchmark_result.GetDurationMs(), 10.0);
}

TEST(Ec2BenchmarkResultTest, Exceptions) {
  const size_t repetition_count = 2;
  const size_t invocation_count = 3;

  Ec2BenchmarkResult benchmark_result(repetition_count, invocation_count);

  for (size_t i = 0; i < repetition_count; ++i) {
    for (size_t j = 0; j < invocation_count; ++j) {
      EXPECT_ANY_THROW(benchmark_result.FinalizeRepetition(i, 1.0));
      EXPECT_ANY_THROW(benchmark_result.FinalizeResult(10.0));
      EXPECT_ANY_THROW(benchmark_result.GetDurationMs());
      benchmark_result.RegisterInstanceLaunch(i, std::to_string(j), 1.0);
      benchmark_result.UpdateCooldown(i, std::to_string(j), 2.0);
    }

    EXPECT_NO_THROW(benchmark_result.FinalizeRepetition(i, 1.0));
  }

  EXPECT_NO_THROW(benchmark_result.FinalizeResult(10.0));
}

}  // namespace skyrise
