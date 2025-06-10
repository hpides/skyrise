#include <gtest/gtest.h>
#include <sys/utsname.h>

#include "client/coordinator_client.hpp"
#include "micro_benchmark/ec2/ec2_benchmark_runner.hpp"
#include "micro_benchmark/ec2/ec2_startup_benchmark.hpp"
#include "micro_benchmark/ec2/ec2_storage_benchmark.hpp"
#include "micro_benchmark/lambda/lambda_availability_benchmark.hpp"
#include "micro_benchmark/lambda/lambda_benchmark_runner.hpp"
#include "micro_benchmark/lambda/lambda_colocation_benchmark.hpp"
#include "micro_benchmark/lambda/lambda_idletime_benchmark.hpp"
#include "micro_benchmark/lambda/lambda_network_benchmark.hpp"
#include "micro_benchmark/lambda/lambda_startup_latency_benchmark.hpp"
#include "micro_benchmark/lambda/lambda_startup_throughput_benchmark.hpp"
#include "micro_benchmark/lambda/lambda_storage_benchmark.hpp"
#include "micro_benchmark/lambda/lambda_warmup_benchmark.hpp"
#include "micro_benchmark/lambda/lambda_warmup_continuous_benchmark.hpp"
#include "testing/aws_test.hpp"
#include "utils/costs/cost_calculator.hpp"
#include "utils/profiling/function_host_information.hpp"

namespace skyrise {

class AwsMicroBenchmarkTest : public ::testing::Test {
 protected:
  void SetUp() override {
    client_ = std::make_shared<CoordinatorClient>();
    cost_calculator_ = std::make_shared<CostCalculator>(client_->GetPricingClient(), client_->GetClientRegion());

    ec2_benchmark_runner_ = std::make_shared<Ec2BenchmarkRunner>(client_->GetEc2Client(), client_->GetSsmClient());
    lambda_benchmark_runner_ =
        std::make_shared<LambdaBenchmarkRunner>(client_->GetIamClient(), client_->GetLambdaClient(),
                                                client_->GetSqsClient(), cost_calculator_, client_->GetClientRegion());
  }

  [[nodiscard]] const CoordinatorClient& GetClient() const { return *client_; }
  [[nodiscard]] std::shared_ptr<CostCalculator> GetCostCalculator() const { return cost_calculator_; }
  [[nodiscard]] std::shared_ptr<Ec2BenchmarkRunner> GetEc2BenchmarkRunner() const { return ec2_benchmark_runner_; }
  [[nodiscard]] std::shared_ptr<LambdaBenchmarkRunner> GetLambdaBenchmarkRunner() const {
    return lambda_benchmark_runner_;
  }

 private:
  const AwsApi aws_api_;

  std::shared_ptr<CoordinatorClient> client_;

  std::shared_ptr<CostCalculator> cost_calculator_;
  std::shared_ptr<Ec2BenchmarkRunner> ec2_benchmark_runner_;
  std::shared_ptr<LambdaBenchmarkRunner> lambda_benchmark_runner_;
};

TEST_F(AwsMicroBenchmarkTest, Ec2StartupBenchmark) {
  const std::vector<Ec2InstanceType> instance_types = {Ec2InstanceType::kT4GNano};
  const std::vector<size_t> concurrent_instance_count = {2};
  const size_t repetition_count = 1;
  const bool warmup_flag = true;

  auto benchmark =
      std::make_shared<Ec2StartupBenchmark>(instance_types, concurrent_instance_count, repetition_count, warmup_flag);
  const auto benchmark_result = benchmark->Run(GetEc2BenchmarkRunner());
  EXPECT_EQ(benchmark_result.GetLength(), 1);
}

TEST_F(AwsMicroBenchmarkTest, Ec2StorageBenchmark) {
  utsname cpu_information{};
  uname(&cpu_information);
  std::vector<Ec2InstanceType> instance_types;
  cpu_information.machine == std::string("aarch64") ? instance_types = {Ec2InstanceType::kT4GNano}
                                                    : instance_types = {Ec2InstanceType::kT3Nano};
  const std::vector<size_t> concurrent_instance_count = {1};
  const size_t repetition_count = 1;
  const std::vector<size_t> after_repetition_delay_min = {0};
  const size_t object_size_kb = 1000;
  const size_t object_count = 2;
  const std::vector<StorageSystem> storage_types = {StorageSystem::kS3};
  const bool enable_s3_eoz = false;
  const bool enable_vpc = false;

  auto benchmark = std::make_shared<Ec2StorageBenchmark>(instance_types, concurrent_instance_count, repetition_count,
                                                         after_repetition_delay_min, object_size_kb, object_count,
                                                         storage_types, enable_s3_eoz, enable_vpc);
  const auto benchmark_result = benchmark->Run(GetEc2BenchmarkRunner());
  EXPECT_EQ(benchmark_result.GetLength(), 1);
}

TEST_F(AwsMicroBenchmarkTest, LambdaAvailabilityBenchmark) {
  const std::vector<size_t> function_instance_sizes_mb = {128};
  const std::vector<size_t> concurrent_instance_counts = {16};
  const size_t repetition_count = 2;
  const std::vector<size_t> after_repetition_delay_min = {1};

  auto benchmark = std::make_shared<skyrise::LambdaAvailabilityBenchmark>(
      GetCostCalculator(), function_instance_sizes_mb, concurrent_instance_counts, repetition_count,
      after_repetition_delay_min);

  const auto benchmark_result = benchmark->Run(GetLambdaBenchmarkRunner());
  EXPECT_EQ(benchmark_result.GetLength(), 1);
}

TEST_F(AwsMicroBenchmarkTest, LambdaColocationBenchmark) {
  const std::vector<size_t> function_instance_sizes_mb = {128};
  const std::vector<size_t> concurrent_instance_counts = {16};
  const size_t repetition_count = 1;

  auto benchmark = std::make_shared<skyrise::LambdaColocationBenchmark>(GetCostCalculator(), function_instance_sizes_mb,
                                                                        concurrent_instance_counts, repetition_count);

  const auto benchmark_result = benchmark->Run(GetLambdaBenchmarkRunner());
  EXPECT_EQ(benchmark_result.GetLength(), 1);
}

TEST_F(AwsMicroBenchmarkTest, LambdaIdletimeBenchmark) {
  const std::vector<size_t> function_instance_sizes_mb = {128};
  const std::vector<size_t> concurrent_instance_counts = {16};
  const size_t repetition_count = 1;
  const std::vector<size_t> after_repetition_delays_min = {1};

  auto benchmark = std::make_shared<skyrise::LambdaIdletimeBenchmark>(GetCostCalculator(), function_instance_sizes_mb,
                                                                      concurrent_instance_counts, repetition_count,
                                                                      after_repetition_delays_min);

  const auto benchmark_result = benchmark->Run(GetLambdaBenchmarkRunner());
  EXPECT_EQ(benchmark_result.GetLength(), 1);
}

TEST_F(AwsMicroBenchmarkTest, LambdaNetworkThroughputBenchmark) {
  const std::vector<size_t> function_instance_sizes_mb = {128};
  const std::vector<size_t> concurrent_instance_counts = {1};
  const size_t repetition_count = 1;
  const size_t duration_s = 10;
  const size_t report_interval_ms = 1000;
  const size_t target_throughput_mbps = std::numeric_limits<size_t>::max();  // unlimited
  const bool enable_download = true;
  const bool enable_upload = false;
  const DistinctFunctionPerRepetition use_distinct_function_per_repetition = DistinctFunctionPerRepetition::kNo;
  const bool enable_vpc = false;
  const int message_size_kb = 16;

  auto benchmark = std::make_shared<skyrise::LambdaNetworkBenchmark>(
      GetCostCalculator(), GetClient().GetEc2Client(), GetClient().GetSqsClient(), function_instance_sizes_mb,
      concurrent_instance_counts, repetition_count, duration_s, report_interval_ms, target_throughput_mbps,
      enable_download, enable_upload, use_distinct_function_per_repetition, enable_vpc, message_size_kb);

  const auto benchmark_result = benchmark->Run(GetLambdaBenchmarkRunner());
  EXPECT_EQ(benchmark_result.GetLength(), 1);
}

TEST_F(AwsMicroBenchmarkTest, LambdaStartupLatencyBenchmark) {
  const std::vector<size_t> function_instance_sizes_mb = {128};
  const std::vector<size_t> concurrent_instance_counts = {16};
  const size_t repetition_count = 1;
  const std::vector<DeploymentType> deployment_types = {DeploymentType::kLocalZipFile};
  const std::vector<bool> warmup_flags = {false};

  auto benchmark = std::make_shared<skyrise::LambdaStartupLatencyBenchmark>(
      GetCostCalculator(), GetClient().GetS3Client(), GetClient().GetXRayClient(), function_instance_sizes_mb,
      concurrent_instance_counts, repetition_count, deployment_types);

  const auto benchmark_result = benchmark->Run(GetLambdaBenchmarkRunner());
  EXPECT_EQ(benchmark_result.GetLength(), 12);
}

TEST_F(AwsMicroBenchmarkTest, LambdaStartupThroughputBenchmark) {
  const std::vector<size_t> function_instance_sizes_mb = {128};
  const std::vector<size_t> concurrent_instance_counts = {16};
  const size_t repetition_count = 1;
  const std::vector<size_t> function_payload_byte_sizes = {128};

  auto benchmark = std::make_shared<skyrise::LambdaStartupThroughputBenchmark>(
      GetCostCalculator(), function_instance_sizes_mb, concurrent_instance_counts, repetition_count,
      function_payload_byte_sizes);

  const auto benchmark_result = benchmark->Run(GetLambdaBenchmarkRunner());
  EXPECT_EQ(benchmark_result.GetLength(), 2);
}

TEST_F(AwsMicroBenchmarkTest, LambdaStorageLatencyBenchmark) {
  const std::vector<size_t> function_instance_sizes_mb = {128};
  const std::vector<size_t> concurrent_instance_counts = {1};
  const size_t repetition_count = 1;
  const std::vector<size_t> after_repetition_delay_min = {0};
  const size_t object_size_kb = 1;
  const size_t object_count = 2;
  const bool enable_writes = true;
  const bool enable_reads = true;
  const std::vector<StorageSystem> storage_types = {StorageSystem::kS3};
  const bool enable_s3_eoz = false;
  const bool enable_vpc = false;

  auto benchmark = std::make_shared<skyrise::LambdaStorageBenchmark>(
      GetCostCalculator(), GetClient().GetDynamoDbClient(), GetClient().GetEfsClient(), GetClient().GetS3Client(),
      GetClient().GetSqsClient(), function_instance_sizes_mb, concurrent_instance_counts, repetition_count,
      after_repetition_delay_min, object_size_kb, object_count, enable_writes, enable_reads, storage_types,
      enable_s3_eoz, enable_vpc);

  const auto benchmark_result = benchmark->Run(GetLambdaBenchmarkRunner());
  EXPECT_EQ(benchmark_result.GetLength(), 2);
}

TEST_F(AwsMicroBenchmarkTest, LambdaStorageThroughputBenchmark) {
  const std::vector<size_t> function_instance_sizes_mb = {128};
  const std::vector<size_t> concurrent_instance_counts = {1};
  const size_t repetition_count = 1;
  const std::vector<size_t> after_repetition_delay_min = {0};
  const size_t object_sizes_kb = 1024;
  const size_t object_count = 2;
  const bool enable_writes = true;
  const bool enable_reads = true;
  const std::vector<StorageSystem> storage_types = {StorageSystem::kS3};
  const bool enable_s3_eoz = false;
  const bool enable_vpc = false;

  auto benchmark = std::make_shared<skyrise::LambdaStorageBenchmark>(
      GetCostCalculator(), GetClient().GetDynamoDbClient(), GetClient().GetEfsClient(), GetClient().GetS3Client(),
      GetClient().GetSqsClient(), function_instance_sizes_mb, concurrent_instance_counts, repetition_count,
      after_repetition_delay_min, object_sizes_kb, object_count, enable_writes, enable_reads, storage_types,
      enable_s3_eoz, enable_vpc);

  const auto benchmark_result = benchmark->Run(GetLambdaBenchmarkRunner());
  EXPECT_EQ(benchmark_result.GetLength(), 2);
}

TEST_F(AwsMicroBenchmarkTest, LambdaStorageParallelThroughputBenchmark) {
  const std::vector<size_t> function_instance_sizes_mb = {128};
  const std::vector<size_t> concurrent_instance_counts = {16};
  const size_t repetition_count = 1;
  const std::vector<size_t> after_repetition_delay_min = {0};
  const size_t object_sizes_kb = 1024;
  const size_t object_count = 2;
  const bool enable_writes = true;
  const bool enable_reads = true;
  const std::vector<StorageSystem> storage_types = {StorageSystem::kS3};
  const bool enable_s3_eoz = false;
  const bool enable_vpc = false;

  auto benchmark = std::make_shared<skyrise::LambdaStorageBenchmark>(
      GetCostCalculator(), GetClient().GetDynamoDbClient(), GetClient().GetEfsClient(), GetClient().GetS3Client(),
      GetClient().GetSqsClient(), function_instance_sizes_mb, concurrent_instance_counts, repetition_count,
      after_repetition_delay_min, object_sizes_kb, object_count, enable_writes, enable_reads, storage_types,
      enable_s3_eoz, enable_vpc);

  const auto benchmark_result = benchmark->Run(GetLambdaBenchmarkRunner());
  EXPECT_EQ(benchmark_result.GetLength(), 2);
}

TEST_F(AwsMicroBenchmarkTest, LambdaWarmupBenchmark) {
  const std::vector<size_t> function_instance_sizes_mb = {128};
  const std::vector<size_t> concurrent_instance_counts = {16};
  const size_t repetition_count = 1;
  const bool enable_provisioned_concurrency = false;
  const std::vector<double> provisioning_factors = {1.2};

  auto benchmark = std::make_shared<skyrise::LambdaWarmupBenchmark>(
      GetCostCalculator(), function_instance_sizes_mb, concurrent_instance_counts, repetition_count,
      enable_provisioned_concurrency, provisioning_factors);

  const auto benchmark_result = benchmark->Run(GetLambdaBenchmarkRunner());
  EXPECT_EQ(benchmark_result.GetLength(), 1);
}

TEST_F(AwsMicroBenchmarkTest, LambdaWarmupContinuousBenchmark) {
  const std::vector<size_t> function_instance_sizes_mb = {128};
  const std::vector<size_t> concurrent_instance_counts = {16};
  const size_t repetition_count = 2;
  const std::vector<double> provisioning_factors = {1.2};
  const std::vector<size_t> warmup_intervals_min = {1};

  auto benchmark = std::make_shared<skyrise::LambdaWarmupContinuousBenchmark>(
      GetCostCalculator(), function_instance_sizes_mb, concurrent_instance_counts, repetition_count,
      provisioning_factors, warmup_intervals_min);

  const auto benchmark_result = benchmark->Run(GetLambdaBenchmarkRunner());
  EXPECT_EQ(benchmark_result.GetLength(), 1);
}

}  // namespace skyrise
