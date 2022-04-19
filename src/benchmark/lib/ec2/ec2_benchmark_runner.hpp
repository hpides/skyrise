#pragma once

#include <unordered_map>

#include <aws/ec2/EC2Client.h>
#include <aws/ec2/model/RunInstancesRequest.h>

#include "abstract_benchmark_runner.hpp"
#include "client/client.hpp"
#include "ec2_benchmark_config.hpp"
#include "ec2_benchmark_result.hpp"

namespace skyrise {

class Ec2BenchmarkRunner : public AbstractBenchmarkRunner {
 public:
  Ec2BenchmarkRunner(std::shared_ptr<const Aws::EC2::EC2Client> ec2_client);
  std::shared_ptr<Ec2BenchmarkResult> RunEc2Config(const std::shared_ptr<Ec2BenchmarkConfig>& config);

 protected:
  void Setup() override;
  void Teardown() override;
  std::shared_ptr<AbstractBenchmarkResult> OnRunConfig() override;
  void TerminateInstances();

  const std::shared_ptr<const Aws::EC2::EC2Client> ec2_client_;
  std::vector<std::vector<Aws::EC2::Model::RunInstancesRequest>> run_instance_requests_;

  std::shared_ptr<Ec2BenchmarkConfig> typed_config_;
  std::unordered_map<Aws::String, bool> running_instances_;
  std::shared_ptr<Ec2BenchmarkResult> result_;

  // AWS recommends to only run 100 EC2 instances per request:
  // https://sdk.amazonaws.com/cpp/api/LATEST/class_aws_1_1_e_c2_1_1_e_c2_client.html#aac0e6842f8753e05bd6c2f5ae4bf9e94
  const size_t kMaxInstancesPerRequest = 100;
  // TODO(d-justen): Fetch the latest available AL2 64-bit x86 image ID.
  const Aws::String kAmazonLinux2ImageId = "ami-02e136e904f3da870";
};

}  // namespace skyrise
