#pragma once

#include <memory>

#include <aws/core/utils/json/JsonSerializer.h>

#include "lambda_benchmark.hpp"
#include "lambda_benchmark_config.hpp"
#include "lambda_benchmark_output.hpp"
#include "lambda_benchmark_types.hpp"

namespace skyrise {

class LambdaStorageBenchmark : public LambdaBenchmark {
 public:
  LambdaStorageBenchmark(std::shared_ptr<const CostCalculator> cost_calculator,
                         std::shared_ptr<const Aws::DynamoDB::DynamoDBClient> dynamodb_client,
                         std::shared_ptr<const Aws::EFS::EFSClient> efs_client,
                         std::shared_ptr<const Aws::S3::S3Client> s3_client,
                         std::shared_ptr<const Aws::SQS::SQSClient> sqs_client,
                         const std::vector<size_t>& function_instance_sizes_mb,
                         const std::vector<size_t>& concurrent_instance_counts, const size_t repetition_count,
                         const std::vector<size_t>& after_repetition_delays_min, const size_t object_size_kb,
                         const size_t object_count, const bool enable_writes, const bool enable_reads,
                         const std::vector<StorageSystem>& storage_types, const bool enable_s3_eoz,
                         const bool enable_vpc);

  const Aws::String& Name() const override;

 private:
  void Setup();

  void Teardown();

  static std::vector<std::shared_ptr<Aws::IOStream>> GeneratePayloads(
      const LambdaStorageBenchmarkParameters& parameters, const std::string& sqs_queue_url);

  static LambdaBenchmarkOutput& AddParameters(LambdaBenchmarkOutput& output,
                                              const LambdaStorageBenchmarkParameters& parameters);

  Aws::Utils::Json::JsonValue GenerateResultOutput(const std::shared_ptr<LambdaBenchmarkResult>& result,
                                                   const LambdaStorageBenchmarkParameters& parameters) const;

  std::string SetupSqsQueue() const;

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> OnRun(const std::shared_ptr<LambdaBenchmarkRunner>& runner) override;

  const std::shared_ptr<const Aws::DynamoDB::DynamoDBClient> dynamodb_client_;
  const std::shared_ptr<const Aws::EFS::EFSClient> efs_client_;
  const std::shared_ptr<const Aws::S3::S3Client> s3_client_;
  const std::shared_ptr<const Aws::SQS::SQSClient> sqs_client_;
  std::vector<std::pair<LambdaStorageBenchmarkParameters, std::shared_ptr<LambdaBenchmarkConfig>>> configs_;
};

}  // namespace skyrise
