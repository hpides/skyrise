#include "benchmark_runner.hpp"

#include <aws/core/Aws.h>
#include <gtest/gtest.h>

#include "benchmark_config.hpp"
#include "client/client.hpp"
#include "lib/testing/aws_test.hpp"
#include "utils/string.hpp"

namespace skyrise {

const std::string kFunctionName = "skyriseFunctionSimple";
const size_t kMemorySize = 128;

class BenchmarkRunnerTest : public ::testing::Test {
 protected:
  void RunConfig(const BenchmarkConfig& benchmark_config) {
    const auto result = benchmark_runner_.RunConfig(benchmark_config);
    ASSERT_TRUE(result);

    const auto invocation_results = result->GetInvocationResults();
    EXPECT_EQ(invocation_results.size(), benchmark_config.repetition_count_);

    if (benchmark_config.warm_up_ == WarmUp::kNone) {
      EXPECT_EQ(result->GetOverallFunctionWarmUpCost(), 0.0L);
    } else {
      EXPECT_GT(result->GetOverallFunctionWarmUpCost(), 0.0L);
    }

    const auto& warm_up_costs = result->GetFunctionWarmUpCosts();
    ASSERT_EQ(warm_up_costs.size(), invocation_results.size());

    for (size_t i = 0; i < invocation_results.size(); i++) {
      EXPECT_TRUE(result->HasRepetitionFinished(i));

      if (i > 0 && benchmark_config.warm_up_ == WarmUp::kDefaultOncePerRepetition) {
        EXPECT_GT(warm_up_costs[i], 0.0L);
      }

      const auto& repetition_results = invocation_results[i];
      EXPECT_EQ(repetition_results.size(), benchmark_config.concurrent_invocation_count_);

      for (const auto& single_result : repetition_results) {
        EXPECT_FALSE(single_result.first.empty());
        EXPECT_TRUE(single_result.second.success);
        EXPECT_TRUE(single_result.second.finished);

        const auto& invoke_result = single_result.second.invoke_result;

        if (benchmark_config.use_event_queue_ == UseEventQueue::kNo) {
          EXPECT_FALSE(invoke_result->GetLogResult().empty());

          std::string payload = StreamToString(&invoke_result->GetPayload());
          EXPECT_FALSE(payload.empty());
        } else {
          EXPECT_FALSE(single_result.second.sqs_message_body.empty());
        }
      }
    }
  }

  const AwsAPI aws_api_;
  const std::shared_ptr<Client> clients_ = std::make_shared<Client>();
  BenchmarkRunner benchmark_runner_ = BenchmarkRunner(clients_);
};

TEST_F(BenchmarkRunnerTest, SyncIntegrationTest) {
  size_t after_repetition_count = 0;

  // TODO(d-justen): Test configuration with XRay and S3-resident function packages
  const std::vector<BenchmarkConfig> benchmark_configs{
      BenchmarkConfig(kFunctionName, kMemorySize, 3, 2, WarmUp::kNone, UseOneFunctionPerRepetition::kNo,
                      UseEventQueue::kNo, {}),
      BenchmarkConfig(kFunctionName, kMemorySize, 1, 2, WarmUp::kDefault, UseOneFunctionPerRepetition::kNo,
                      UseEventQueue::kNo, {[&]() { ++after_repetition_count; }}),
      BenchmarkConfig(kFunctionName, kMemorySize, 2, 1, WarmUp::kDefaultOncePerRepetition,
                      UseOneFunctionPerRepetition::kYes, UseEventQueue::kNo, {})};

  for (const auto& benchmark_config : benchmark_configs) {
    RunConfig(benchmark_config);
  }

  EXPECT_EQ(after_repetition_count, 1);
}

TEST_F(BenchmarkRunnerTest, AsyncIntegrationTest) {
  const BenchmarkConfig benchmark_config(kFunctionName, kMemorySize, 3, 2, WarmUp::kNone,
                                         UseOneFunctionPerRepetition::kNo, UseEventQueue::kYes, {});

  RunConfig(benchmark_config);
}

}  // namespace skyrise
