#pragma once

#include <aws/s3/S3Client.h>

#include "abstract_benchmark.hpp"
#include "aws/sqs/model/Message.h"
#include "client/base_client.hpp"
#include "ec2_benchmark_config.hpp"
#include "ec2_benchmark_result.hpp"
#include "ec2_startup_benchmark.hpp"
#include "storage/backend/s3_storage.hpp"

namespace skyrise {

class Ec2StorageBenchmark : public AbstractBenchmark {
 public:
  Ec2StorageBenchmark(const std::vector<Ec2InstanceType>& instance_types,
                      const std::vector<size_t>& concurrent_instance_counts, const size_t repetition_count,
                      const std::vector<size_t>& after_repetition_delays_min, const size_t object_size_kb,
                      const size_t object_count, const std::vector<StorageSystem>& storage_types,
                      const bool enable_s3_eoz, const bool enable_vpc);

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> Run(
      const std::shared_ptr<AbstractBenchmarkRunner>& benchmark_runner) override;

 protected:
  const Aws::String& Name() const override;

 private:
  void Setup();
  void Teardown();
  std::string SetupSqsQueue() const;
  void UploadLocalFile(const std::string& local_path, const std::string& object_id);
  std::string GenerateUserData(const size_t benchmark_id, const size_t repetition_id, const size_t invocation_id);
  Aws::Utils::Json::JsonValue WaitForRepetitionEnd(const size_t benchmark_id, const size_t repetition_id);
  static Aws::Utils::Json::JsonValue ExtractResponse(const std::string& string_response);
  Aws::Utils::Json::JsonValue GenerateResultOutput(const std::shared_ptr<Ec2BenchmarkResult>& repetition_result,
                                                   const Ec2StorageBenchmarkParameters& parameters);

  std::shared_ptr<BaseClient> client_;
  std::vector<std::pair<std::shared_ptr<Ec2BenchmarkConfig>, Ec2StorageBenchmarkParameters>> benchmark_configs_;
  std::vector<std::shared_ptr<Ec2BenchmarkResult>> benchmark_results_;
  const size_t object_size_kb_;
  const size_t object_count_;
};

}  // namespace skyrise
