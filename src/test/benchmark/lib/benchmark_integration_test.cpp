#include <gtest/gtest.h>

#include "benchmark_helper.hpp"
#include "client/client.hpp"
#include "lambda/function_colocation_benchmark.hpp"
#include "lambda/function_warm_up_benchmark.hpp"
#include "lambda/function_warm_up_continuous_benchmark.hpp"
#include "lambda/idle_availability_benchmark.hpp"
#include "lambda/idle_lifetime_benchmark.hpp"
#include "lambda/invocation_latency_benchmark.hpp"
#include "lambda/invocation_throughput_benchmark.hpp"
#include "lambda/lambda_benchmark.hpp"
#include "lambda/lambda_benchmark_runner.hpp"
#include "lambda/network_latency_benchmark.hpp"
#include "lambda/network_throughput_benchmark.hpp"
#include "lambda/network_throughput_parallel_benchmark.hpp"
#include "lib/testing/aws_test.hpp"
#include "utils/costs/cost_calculator.hpp"

namespace skyrise {

class AwsBenchmarkIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    client_ = std::make_shared<Client>();

    cost_calculator_ = std::make_shared<CostCalculator>(client_->GetPricingClient(), client_->GetClientRegion());
    benchmark_runner_ = std::make_shared<LambdaBenchmarkRunner>(client_->GetIAMClient(), client_->GetLambdaClient(),
                                                                client_->GetSQSClient(), cost_calculator_);
    benchmark_helper_ = std::make_shared<BenchmarkHelper>(client_->GetS3Client());
  }

  [[nodiscard]] const Client& GetClient() const { return *client_; }

  [[nodiscard]] std::shared_ptr<CostCalculator> GetCostCalculator() const { return cost_calculator_; }
  [[nodiscard]] std::shared_ptr<LambdaBenchmarkRunner> GetBenchmarkRunner() const { return benchmark_runner_; }
  [[nodiscard]] std::shared_ptr<BenchmarkHelper> GetBenchmarkHelper() const { return benchmark_helper_; }

 private:
  const AwsApi aws_api_;

  std::shared_ptr<Client> client_;

  std::shared_ptr<CostCalculator> cost_calculator_;
  std::shared_ptr<LambdaBenchmarkRunner> benchmark_runner_;
  std::shared_ptr<BenchmarkHelper> benchmark_helper_;
};

TEST_F(AwsBenchmarkIntegrationTest, FunctionColocationBenchmark) {
  const std::vector<size_t> function_instance_mb_sizes = {128};
  const std::vector<size_t> invocation_counts = {16};
  const std::vector<size_t> sleep_min_durations = {1};
  const size_t repetition_count = 1;

  auto benchmark = std::make_shared<skyrise::FunctionColocationBenchmark>(
      GetCostCalculator(), function_instance_mb_sizes, invocation_counts, sleep_min_durations, repetition_count);

  const auto benchmark_result = benchmark->Run(GetBenchmarkRunner());
  EXPECT_EQ(benchmark_result.GetLength(), 1);
}

TEST_F(AwsBenchmarkIntegrationTest, FunctionWarmUpBenchmark) {
  const std::vector<size_t> function_instance_mb_sizes = {128};
  const std::vector<size_t> invocation_counts = {16};
  const std::vector<size_t> sleep_ms_durations = {400};
  const std::vector<double> provisioning_factors = {1.2};
  const bool enable_provisioned_concurrency = false;
  const size_t repetition_count = 1;

  auto benchmark = std::make_shared<skyrise::FunctionWarmUpBenchmark>(
      GetCostCalculator(), function_instance_mb_sizes, invocation_counts, sleep_ms_durations, provisioning_factors,
      enable_provisioned_concurrency, repetition_count);

  const auto benchmark_result = benchmark->Run(GetBenchmarkRunner());
  EXPECT_EQ(benchmark_result.GetLength(), 1);
}

// TODO(maltenbergert): Add test for FunctionWarmUpContinuousBenchmark

TEST_F(AwsBenchmarkIntegrationTest, IdleAvailabilityBenchmark) {
  const std::vector<size_t> function_instance_mb_sizes = {128};
  const std::vector<size_t> invocation_counts = {16};
  const std::vector<size_t> sleep_min_durations = {1};
  const size_t repetition_count = 2;

  auto benchmark = std::make_shared<skyrise::IdleAvailabilityBenchmark>(
      GetCostCalculator(), function_instance_mb_sizes, invocation_counts, sleep_min_durations, repetition_count);

  const auto benchmark_result = benchmark->Run(GetBenchmarkRunner());
  EXPECT_EQ(benchmark_result.GetLength(), 1);
}

TEST_F(AwsBenchmarkIntegrationTest, skyriseBenchmarkIdleLifetime) {
  const std::vector<size_t> function_instance_mb_sizes = {128};
  const std::vector<size_t> invocation_counts = {16};
  const std::vector<size_t> sleep_min_durations = {1, 2};
  const size_t repetition_count = 1;

  auto benchmark = std::make_shared<skyrise::IdleLifetimeBenchmark>(
      GetCostCalculator(), function_instance_mb_sizes, invocation_counts, sleep_min_durations, repetition_count);

  const auto benchmark_result = benchmark->Run(GetBenchmarkRunner());
  EXPECT_EQ(benchmark_result.GetLength(), 2);
}

// TODO(tobodner): Fix sporadic segmentation fault and re-enable test.
TEST_F(AwsBenchmarkIntegrationTest, DISABLED_InvocationLatencyBenchmark) {
  const std::vector<size_t> function_instance_mb_sizes = {128};
  const std::vector<size_t> invocation_counts = {16};
  const std::vector<bool> warm_modes = {true, false};
  const std::vector<size_t> sleep_ms_durations = {4000};
  const size_t repetition_count = 1;

  auto benchmark = std::make_shared<skyrise::InvocationLatencyBenchmark>(
      GetClient().GetXRayClient(), GetBenchmarkHelper(), GetCostCalculator(), function_instance_mb_sizes,
      invocation_counts, warm_modes, sleep_ms_durations, repetition_count);

  const auto benchmark_result = benchmark->Run(GetBenchmarkRunner());
  EXPECT_EQ(benchmark_result.GetLength(), 26);
}

TEST_F(AwsBenchmarkIntegrationTest, InvocationThroughputBenchmark) {
  const std::vector<size_t> function_instance_mb_sizes = {128};
  const std::vector<size_t> invocation_counts = {16};
  const std::vector<size_t> function_payload_byte_sizes = {128};
  const size_t repetition_count = 1;

  auto benchmark = std::make_shared<skyrise::InvocationThroughputBenchmark>(
      GetCostCalculator(), function_instance_mb_sizes, invocation_counts, function_payload_byte_sizes,
      repetition_count);

  const auto benchmark_result = benchmark->Run(GetBenchmarkRunner());
  EXPECT_EQ(benchmark_result.GetLength(), 2);
}

TEST_F(AwsBenchmarkIntegrationTest, NetworkLatencyBenchmark) {
  const std::vector<size_t> function_instance_mb_sizes = {128};
  const std::vector<size_t> object_byte_sizes = {1024};
  const std::vector<size_t> batch_sizes = {2};
  const size_t repetition_count = 1;

  auto benchmark = std::make_shared<skyrise::NetworkLatencyBenchmark>(GetBenchmarkHelper(), GetCostCalculator(),
                                                                      function_instance_mb_sizes, object_byte_sizes,
                                                                      batch_sizes, repetition_count);

  const auto benchmark_result = benchmark->Run(GetBenchmarkRunner());
  EXPECT_EQ(benchmark_result.GetLength(), 2);
}

TEST_F(AwsBenchmarkIntegrationTest, NetworkThroughputBenchmark) {
  const std::vector<size_t> function_instance_mb_sizes = {128};
  const std::vector<size_t> object_byte_sizes = {16384};
  const std::vector<size_t> batch_sizes = {2};
  const std::vector<size_t> thread_counts = {2};

  const size_t repetition_count = 1;

  auto benchmark = std::make_shared<skyrise::NetworkThroughputBenchmark>(GetBenchmarkHelper(), GetCostCalculator(),
                                                                         function_instance_mb_sizes, object_byte_sizes,
                                                                         batch_sizes, thread_counts, repetition_count);

  const auto benchmark_result = benchmark->Run(GetBenchmarkRunner());
  EXPECT_EQ(benchmark_result.GetLength(), 2);
}

TEST_F(AwsBenchmarkIntegrationTest, NetworkThroughputParallelBenchmark) {
  const std::vector<size_t> function_instance_mb_sizes = {128};
  const std::vector<size_t> object_byte_sizes = {16384};
  const std::vector<size_t> batch_sizes = {2};
  const std::vector<size_t> thread_counts = {2};
  const std::vector<size_t> invocation_counts = {16};
  const std::vector<size_t> bucket_counts = {2};

  const bool enable_reads = true;
  const size_t repetition_count = 1;

  auto benchmark = std::make_shared<skyrise::NetworkThroughputParallelBenchmark>(
      GetBenchmarkHelper(), GetCostCalculator(), function_instance_mb_sizes, object_byte_sizes, batch_sizes,
      thread_counts, invocation_counts, bucket_counts, enable_reads, repetition_count);

  const auto benchmark_result = benchmark->Run(GetBenchmarkRunner());
  EXPECT_EQ(benchmark_result.GetLength(), 2);
}

}  // namespace skyrise
