#include "micro_benchmark/ec2/ec2_benchmark_runner.hpp"

#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>

#include "client/coordinator_client.hpp"
#include "micro_benchmark/ec2/ec2_benchmark_config.hpp"
#include "testing/aws_test.hpp"

namespace skyrise {

class AwsEc2BenchmarkRunnerTest : public ::testing::Test {
 protected:
  void RunConfig(const std::shared_ptr<Ec2BenchmarkConfig>& benchmark_config) {
    const auto result = benchmark_runner_.RunEc2Config(benchmark_config);
    ASSERT_NE(result, nullptr);

    EXPECT_TRUE(result->IsResultComplete());
    EXPECT_EQ(result->GetRepetitionResults().size(), benchmark_config->GetRepetitionCount());
    EXPECT_GT(result->GetDurationMs(), 0.0);

    for (const auto& repetition : result->GetRepetitionResults()) {
      EXPECT_EQ(repetition.invocation_results.size(), benchmark_config->GetConcurrentInstanceCount());

      for (const auto& [instance_id, invocation_result] : repetition.invocation_results) {
        EXPECT_GT(invocation_result.instance_transitions.running_state.coldstart_running_state_duration_ms, 0.0);
        EXPECT_GT(invocation_result.instance_transitions.coldstart_duration_ms, 0.0);
        EXPECT_TRUE(invocation_result.instance_transitions.termination_duration_ms.has_value());
      }
    }
  }

  const AwsApi aws_api_;
  const CoordinatorClient client_;
  Ec2BenchmarkRunner benchmark_runner_ = Ec2BenchmarkRunner(client_.GetEc2Client(), client_.GetSsmClient());
};

TEST_F(AwsEc2BenchmarkRunnerTest, GetLatestAmi) {
  std::vector<Ec2InstanceType> instance_types{Ec2InstanceType::kT3Nano, Ec2InstanceType::kT4GNano,
                                              Ec2InstanceType::kM6ILarge, Ec2InstanceType::kM6GMedium};

  for (const auto& value : instance_types) {
    ASSERT_NO_THROW(benchmark_runner_.GetLatestAmi(Ec2BenchmarkConfig::ToAwsType(value)));
  }
}

TEST_F(AwsEc2BenchmarkRunnerTest, RunConfig) {
  std::vector<std::shared_ptr<Ec2BenchmarkConfig>> benchmark_configs{
      std::make_shared<Ec2BenchmarkConfig>(Ec2InstanceType::kT4GNano, 1, 1),
      std::make_shared<Ec2BenchmarkConfig>(Ec2InstanceType::kT4GNano, 3, 2),
      std::make_shared<Ec2BenchmarkConfig>(Ec2InstanceType::kM6GMedium, 1, 1)};

  for (const auto& config : benchmark_configs) {
    RunConfig(config);
  }
}
}  // namespace skyrise
