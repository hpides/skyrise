#include "micro_benchmark/lambda/lambda_benchmark_runner.hpp"

#include <gtest/gtest.h>

#include "client/coordinator_client.hpp"
#include "micro_benchmark/lambda/lambda_benchmark_config.hpp"
#include "testing/aws_test.hpp"

namespace skyrise {

namespace {

const std::string kFunctionName = "skyriseSimpleFunction";
constexpr size_t kMemorySize = 128;

}  // namespace

class AwsLambdaBenchmarkRunnerTest : public ::testing::Test {
 protected:
  void RunConfig(const std::shared_ptr<LambdaBenchmarkConfig>& benchmark_config) {
    const auto result = benchmark_runner_.RunLambdaConfig(benchmark_config);
    ASSERT_NE(result, nullptr);

    const auto benchmark_repetitions = result->GetRepetitionResults();
    EXPECT_EQ(benchmark_repetitions.size(), benchmark_config->GetRepetitionCount());

    if (benchmark_config->GetWarmupType() == Warmup::kNo) {
      EXPECT_EQ(result->CalculateWarmupCost(), 0.0L);
    } else {
      EXPECT_GT(result->CalculateWarmupCost(), 0.0L);
    }

    for (size_t i = 0; i < benchmark_repetitions.size(); ++i) {
      EXPECT_TRUE(result->HasRepetitionFinished(i));

      if (i > 0 && benchmark_config->GetWarmupType() == Warmup::kYesOnEveryRepetition) {
        EXPECT_GT(benchmark_repetitions[i].GetWarmupCost(), 0.0L);
      }

      EXPECT_EQ(benchmark_repetitions[i].GetInvocationResults().size(), benchmark_config->GetConcurrentInstanceCount());

      for (const auto& invocation_result : benchmark_repetitions[i].GetInvocationResults()) {
        EXPECT_TRUE(invocation_result.IsSuccess());
        EXPECT_TRUE(invocation_result.IsComplete());

        if (benchmark_config->UseEventQueue() == EventQueue::kNo) {
          EXPECT_TRUE(invocation_result.HasResultLog());
        }
      }
    }
  }

  const AwsApi aws_api_;
  const CoordinatorClient client_;
  const Aws::String client_region_ = client_.GetClientRegion();
  LambdaBenchmarkRunner benchmark_runner_ = LambdaBenchmarkRunner(
      client_.GetIamClient(), client_.GetLambdaClient(), client_.GetSqsClient(),
      std::make_shared<CostCalculator>(client_.GetPricingClient(), client_region_), client_region_);
};

TEST_F(AwsLambdaBenchmarkRunnerTest, RunConfigWithSyncInvocation) {
  size_t after_repetition_count = 0;

  const std::vector<std::shared_ptr<LambdaBenchmarkConfig>> benchmark_configs{
      std::make_shared<LambdaBenchmarkConfig>(LambdaBenchmarkConfig(kFunctionName, "", Warmup::kNo,
                                                                    DistinctFunctionPerRepetition::kNo, EventQueue::kNo,
                                                                    false, kMemorySize, "", 2, 3, {})),
      std::make_shared<LambdaBenchmarkConfig>(
          LambdaBenchmarkConfig(kFunctionName, "", Warmup::kYesOnInitialRepetition, DistinctFunctionPerRepetition::kNo,
                                EventQueue::kNo, false, kMemorySize, "", 2, 1, {[&]() { ++after_repetition_count; }})),
      std::make_shared<LambdaBenchmarkConfig>(
          LambdaBenchmarkConfig(kFunctionName, "", Warmup::kYesOnEveryRepetition, DistinctFunctionPerRepetition::kYes,
                                EventQueue::kNo, false, kMemorySize, "", 2, 1, {}))};

  for (const auto& benchmark_config : benchmark_configs) {
    RunConfig(benchmark_config);
  }

  EXPECT_EQ(after_repetition_count, 1);
}

TEST_F(AwsLambdaBenchmarkRunnerTest, RunConfigWithAsyncInvocation) {
  const auto benchmark_config = std::make_shared<LambdaBenchmarkConfig>(
      LambdaBenchmarkConfig(kFunctionName, "", Warmup::kNo, DistinctFunctionPerRepetition::kNo, EventQueue::kYes, false,
                            kMemorySize, "", 3, 2, {}));

  RunConfig(benchmark_config);
}

}  // namespace skyrise
