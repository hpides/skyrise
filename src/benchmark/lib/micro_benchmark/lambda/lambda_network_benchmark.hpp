#pragma once

#include <aws/core/Aws.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include "lambda_benchmark.hpp"

namespace skyrise {

class LambdaNetworkBenchmark : public LambdaBenchmark {
 public:
  LambdaNetworkBenchmark(std::shared_ptr<const CostCalculator> cost_calculator,
                         const std::shared_ptr<const Aws::EC2::EC2Client>& ec2_client,
                         const std::shared_ptr<const Aws::SQS::SQSClient>& sqs_client,
                         const std::vector<size_t>& function_instance_sizes_mb,
                         const std::vector<size_t>& concurrent_instance_counts, const size_t repetition_count,
                         const size_t duration_s, const size_t report_interval_ms, const size_t target_throughput_mbps,
                         const bool enable_download, const bool enable_upload,
                         const DistinctFunctionPerRepetition distinct_function_per_repetition, const bool enable_vpc,
                         const int message_size_kb);

  const Aws::String& Name() const override;

 private:
  void Setup(const Aws::EC2::Model::InstanceType ec2_instance_type, const size_t ec2_cluster_size,
             const size_t concurrent_instance_count, const size_t repetition_count);

  void Teardown();

  std::vector<std::shared_ptr<Aws::IOStream>> GeneratePayloads(const std::shared_ptr<LambdaBenchmarkConfig>& config);

  Aws::Utils::Json::JsonValue GenerateResultOutput(
      const std::shared_ptr<LambdaBenchmarkResult>& benchmark_result,
      const std::pair<LambdaBenchmarkParameters, std::shared_ptr<LambdaBenchmarkConfig>>& config,
      const std::vector<std::chrono::milliseconds>& ec2_runtimes,
      const Aws::EC2::Model::InstanceType ec2_instance_type) const;

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> OnRun(const std::shared_ptr<LambdaBenchmarkRunner>& runner) override;

  /**
   * Generates a script to install the iperf3 library and start N server processes on EC2 instance startup.
   */
  static std::string GenerateUserDataScript(const size_t start_port, const size_t server_count,
                                            const size_t repetition_count);

  /**
   * Resolves an EC2 c6gn instance that can handle N concurrent invocations.
   * Based on the assumption, that each x of x-large instances resolves to 6 Gigabit network performance.
   */
  std::vector<Aws::EC2::Model::InstanceType> ResolveEc2InstanceTypes(
      const std::vector<size_t>& function_instance_mb_sizes, const std::vector<size_t>& concurrent_invocation_counts);

  std::string SetupSqsQueue() const;

  std::vector<std::pair<LambdaBenchmarkParameters, std::shared_ptr<LambdaBenchmarkConfig>>> configs_;
  const std::shared_ptr<const Aws::EC2::EC2Client> ec2_client_;
  const std::shared_ptr<const Aws::SQS::SQSClient> sqs_client_;
  std::unordered_map<Aws::String, std::pair<std::pair<std::string, size_t>, Aws::String>> ec2_instance_map_;
  std::vector<size_t> ec2_cluster_size_;
  const std::vector<Aws::EC2::Model::InstanceType> ec2_instance_types_;
  std::vector<std::vector<std::chrono::milliseconds>> ec2_runtimes_;
  const size_t duration_s_;
  const double report_interval_ms_;
  const size_t target_throughput_mbps_;
  const bool enable_download_;
  const bool enable_upload_;
  const bool enable_vpc_;
  const int message_size_kb_;
  std::mutex mutex_;
  std::vector<std::string> sqs_queue_urls_;
};

}  // namespace skyrise
