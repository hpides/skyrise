#include "micro_benchmark/ec2/ec2_benchmark_result.hpp"

#include <gtest/gtest.h>

namespace skyrise {

TEST(Ec2BenchmarkResultTest, InitializeAndFinalize) {
  const Aws::EC2::Model::InstanceType instance_type = Aws::EC2::Model::InstanceType::t4g_nano;
  const size_t concurrent_instance_count = 20;
  const size_t repetition_count = 10;
  Ec2BenchmarkResult benchmark_result(instance_type, concurrent_instance_count, repetition_count);

  for (size_t i = 0; i < repetition_count; ++i) {
    for (size_t j = 0; j < concurrent_instance_count; ++j) {
      EXPECT_FALSE(benchmark_result.IsResultComplete());
      EXPECT_FALSE(benchmark_result.IsRepetitionComplete(i));
      benchmark_result.RegisterInstanceColdstart(i, std::to_string(j), 1.0);
      benchmark_result.RegisterInstanceTermination(i, std::to_string(j), 2.0);
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
  const Aws::EC2::Model::InstanceType instance_type = Aws::EC2::Model::InstanceType::t4g_nano;
  const size_t concurrent_instance_count = 3;
  const size_t repetition_count = 2;
  Ec2BenchmarkResult benchmark_result(instance_type, concurrent_instance_count, repetition_count);

  for (size_t i = 0; i < repetition_count; ++i) {
    for (size_t j = 0; j < concurrent_instance_count; ++j) {
      EXPECT_ANY_THROW(benchmark_result.FinalizeRepetition(i, 1.0));
      EXPECT_ANY_THROW(benchmark_result.FinalizeResult(10.0));
      EXPECT_ANY_THROW(benchmark_result.GetDurationMs());
      benchmark_result.RegisterInstanceColdstart(i, std::to_string(j), 1.0);
      benchmark_result.RegisterInstanceTermination(i, std::to_string(j), 2.0);
    }

    EXPECT_NO_THROW(benchmark_result.FinalizeRepetition(i, 1.0));
  }

  EXPECT_NO_THROW(benchmark_result.FinalizeResult(10.0));
}

}  // namespace skyrise
