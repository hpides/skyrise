#include "lambda/lambda_benchmark_runner.hpp"

#include <aws/core/Aws.h>
#include <gtest/gtest.h>

#include "client/client.hpp"
#include "lambda/lambda_benchmark_config.hpp"
#include "lib/testing/aws_test.hpp"

namespace skyrise {

const std::string kFunctionName = "skyriseFunctionSimple";
constexpr size_t kMemorySize = 128;

class AwsLambdaBenchmarkRunnerTest : public ::testing::Test {
 protected:
  void RunConfig(const std::shared_ptr<LambdaBenchmarkConfig>& benchmark_config) {
    const auto result = benchmark_runner_.RunLambdaConfig(benchmark_config);
    ASSERT_NE(result, nullptr);

    const auto benchmark_repetitions = result->GetBenchmarkRepetitions();
    EXPECT_EQ(benchmark_repetitions.size(), benchmark_config->repetition_count_);

    if (benchmark_config->warm_up_ == WarmUp::kNone) {
      EXPECT_EQ(result->GetWarmUpCost(), 0.0L);
    } else {
      EXPECT_GT(result->GetWarmUpCost(), 0.0L);
    }

    for (size_t i = 0; i < benchmark_repetitions.size(); i++) {
      EXPECT_TRUE(result->HasRepetitionFinished(i));

      if (i > 0 && benchmark_config->warm_up_ == WarmUp::kDefaultOncePerRepetition) {
        EXPECT_GT(benchmark_repetitions[i].GetWarmUpCost(), 0.0L);
      }

      EXPECT_EQ(benchmark_repetitions[i].GetInvokeResults().size(), benchmark_config->concurrent_invocation_count_);

      for (const auto& invoke_result : benchmark_repetitions[i].GetInvokeResults()) {
        EXPECT_TRUE(invoke_result.IsSuccess());
        EXPECT_TRUE(invoke_result.IsComplete());

        EXPECT_TRUE(invoke_result.GetResponseBody().KeyExists("success"));
        EXPECT_TRUE(invoke_result.GetResponseBody().GetBool("success"));

        if (benchmark_config->use_event_queue_ == UseEventQueue::kNo) {
          EXPECT_TRUE(invoke_result.HasLogResult());
        }
      }
    }
  }

  const AwsApi aws_api_;
  const Client client_;
  LambdaBenchmarkRunner benchmark_runner_ =
      LambdaBenchmarkRunner(client_.GetIamClient(), client_.GetLambdaClient(), client_.GetSqsClient(),
                            std::make_shared<CostCalculator>(client_.GetPricingClient(), client_.GetClientRegion()));
};

TEST_F(AwsLambdaBenchmarkRunnerTest, SyncIntegrationTest) {
  size_t after_repetition_count = 0;

  // TODO(d-justen): Test configuration with XRay and S3-resident function packages
  const std::vector<std::shared_ptr<LambdaBenchmarkConfig>> benchmark_configs{
      std::make_shared<LambdaBenchmarkConfig>(LambdaBenchmarkConfig(
          kFunctionName, kMemorySize, 3, 2, WarmUp::kNone, UseOneFunctionPerRepetition::kNo, UseEventQueue::kNo, {})),
      std::make_shared<LambdaBenchmarkConfig>(
          LambdaBenchmarkConfig(kFunctionName, kMemorySize, 1, 2, WarmUp::kDefault, UseOneFunctionPerRepetition::kNo,
                                UseEventQueue::kNo, {[&]() { ++after_repetition_count; }})),
      std::make_shared<LambdaBenchmarkConfig>(
          LambdaBenchmarkConfig(kFunctionName, kMemorySize, 2, 1, WarmUp::kDefaultOncePerRepetition,
                                UseOneFunctionPerRepetition::kYes, UseEventQueue::kNo, {}))};

  for (const auto& benchmark_config : benchmark_configs) {
    RunConfig(benchmark_config);
  }

  EXPECT_EQ(after_repetition_count, 1);
}

TEST_F(AwsLambdaBenchmarkRunnerTest, AsyncIntegrationTest) {
  const auto benchmark_config = std::make_shared<LambdaBenchmarkConfig>(LambdaBenchmarkConfig(
      kFunctionName, kMemorySize, 3, 2, WarmUp::kNone, UseOneFunctionPerRepetition::kNo, UseEventQueue::kYes, {}));

  RunConfig(benchmark_config);
}

}  // namespace skyrise
