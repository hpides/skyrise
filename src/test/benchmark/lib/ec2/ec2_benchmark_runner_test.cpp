#include "ec2/ec2_benchmark_runner.hpp"

#include <aws/core/Aws.h>
#include <gtest/gtest.h>

#include "client/client.hpp"
#include "ec2/ec2_benchmark_config.hpp"
#include "lib/testing/aws_test.hpp"

namespace skyrise {

class AwsEc2BenchmarkRunnerTest : public ::testing::Test {
 protected:
  void RunConfig(const std::shared_ptr<Ec2BenchmarkConfig>& benchmark_config) {
    const auto result = benchmark_runner_.RunEc2Config(benchmark_config);
    ASSERT_NE(result, nullptr);

    EXPECT_TRUE(result->IsResultComplete());
    EXPECT_EQ(result->GetRepetitions().size(), benchmark_config->repetition_count_);
    EXPECT_GT(result->GetDurationMs(), 0.0);

    for (const auto& repetition : result->GetRepetitions()) {
      EXPECT_EQ(repetition.launch_durations.size(), benchmark_config->concurrent_invocation_count_);

      for (const auto& launch_duration : repetition.launch_durations) {
        EXPECT_FALSE(launch_duration.instance_id.empty());
        EXPECT_GT(launch_duration.duration_ms, 0.0);
        EXPECT_LT(launch_duration.duration_ms, result->GetDurationMs());
      }
    }
  }

  const AwsApi aws_api_;
  const Client client_;
  Ec2BenchmarkRunner benchmark_runner_ = Ec2BenchmarkRunner(client_.GetEc2Client());
};

TEST_F(AwsEc2BenchmarkRunnerTest, IntegrationTest) {
  std::vector<std::shared_ptr<Ec2BenchmarkConfig>> benchmark_configs = {
      std::make_shared<Ec2BenchmarkConfig>(Ec2InstanceType::kT3Micro, 1, 1),
      std::make_shared<Ec2BenchmarkConfig>(Ec2InstanceType::kT3Micro, 2, 3)};

  for (const auto& config : benchmark_configs) {
    RunConfig(config);
  }
}

}  // namespace skyrise
