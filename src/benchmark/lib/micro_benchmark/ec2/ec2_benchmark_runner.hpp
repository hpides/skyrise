#pragma once

#include <unordered_map>

#include <aws/ec2/EC2Client.h>
#include <aws/ec2/model/RunInstancesRequest.h>
#include <aws/ssm/SSMClient.h>

#include "abstract_benchmark_runner.hpp"
#include "client/coordinator_client.hpp"
#include "ec2_benchmark_config.hpp"
#include "ec2_benchmark_result.hpp"

namespace skyrise {

class Ec2BenchmarkRunner : public AbstractBenchmarkRunner {
 public:
  Ec2BenchmarkRunner(std::shared_ptr<const Aws::EC2::EC2Client> ec2_client,
                     std::shared_ptr<const Aws::SSM::SSMClient> ssm_client, const bool metering = true,
                     const bool introspection = true);

  std::shared_ptr<Ec2BenchmarkResult> RunEc2Config(const std::shared_ptr<Ec2BenchmarkConfig>& config);

  Aws::String GetLatestAmi(Aws::EC2::Model::InstanceType instance_type);

 protected:
  void Setup() override;
  void Teardown() override;

  std::shared_ptr<AbstractBenchmarkResult> OnRunConfig() override;

  void ColdstartInstances(size_t repetition,
                          const std::chrono::time_point<std::chrono::steady_clock>& repetition_begin);
  void StopInstances(const size_t repetition);
  void WarmstartInstances(const size_t repetition);
  void TerminateInstances(const size_t repetition);

  static bool IsSshActive(const std::string& hostname);
  Aws::Vector<Aws::String> GetRunningInstanceIds();
  bool HasRunningInstances();
  bool HasStoppedInstances();

  const std::shared_ptr<const Aws::EC2::EC2Client> ec2_client_;
  const std::shared_ptr<const Aws::SSM::SSMClient> ssm_client_;
  std::shared_ptr<Ec2BenchmarkConfig> typed_config_;
  std::vector<std::vector<Aws::EC2::Model::RunInstancesRequest>> run_instances_requests_;
  std::unordered_map<Aws::String, bool> running_instances_;
  std::shared_ptr<Ec2BenchmarkResult> result_;
};

}  // namespace skyrise
