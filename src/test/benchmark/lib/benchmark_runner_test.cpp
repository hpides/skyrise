#include "benchmark_runner.hpp"

#include <aws/core/Aws.h>
#include <gtest/gtest.h>

#include "benchmark_config.hpp"
#include "client/client.hpp"
#include "lib/testing/aws_test.hpp"

namespace skyrise {

const std::string kFunctionName = "skyriseFunctionSimple";
const size_t kMemorySize = 128;

class BenchmarkRunnerTest : public ::testing::Test {
 protected:
  void RunConfig(const BenchmarkConfig& benchmark_config) {
    const auto result = benchmark_runner_.RunConfig(benchmark_config);
    ASSERT_TRUE(result);

    const auto benchmark_repetitions = result->GetBenchmarkRepetitions();
    EXPECT_EQ(benchmark_repetitions.size(), benchmark_config.repetition_count_);

    if (benchmark_config.warm_up_ == WarmUp::kNone) {
      EXPECT_EQ(result->GetWarmUpCost(), 0.0L);
    } else {
      EXPECT_GT(result->GetWarmUpCost(), 0.0L);
    }

    for (size_t i = 0; i < benchmark_repetitions.size(); i++) {
      EXPECT_TRUE(result->HasRepetitionFinished(i));

      if (i > 0 && benchmark_config.warm_up_ == WarmUp::kDefaultOncePerRepetition) {
        EXPECT_GT(benchmark_repetitions[i].GetWarmUpCost(), 0.0L);
      }

      EXPECT_EQ(benchmark_repetitions[i].GetInvokeResults().size(), benchmark_config.concurrent_invocation_count_);

      const auto response_body = Aws::Utils::Json::JsonValue().AsString("success");

      for (const auto& invoke_result : benchmark_repetitions[i].GetInvokeResults()) {
        EXPECT_TRUE(invoke_result.IsSuccess());
        EXPECT_TRUE(invoke_result.IsComplete());

        EXPECT_EQ(invoke_result.GetResponseBody().WriteCompact(), response_body.View().WriteCompact());

        if (benchmark_config.use_event_queue_ == UseEventQueue::kNo) {
          EXPECT_TRUE(invoke_result.HasLogResult());
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
