#include "ec2_benchmark_runner.hpp"

#include <algorithm>
#include <chrono>
#include <thread>

#include <aws/ec2/model/DescribeInstancesRequest.h>
#include <aws/ec2/model/DescribeInstancesResponse.h>
#include <aws/ec2/model/TerminateInstancesRequest.h>
#include <magic_enum.hpp>

#include "utils/assert.hpp"

namespace skyrise {

Ec2BenchmarkRunner::Ec2BenchmarkRunner(std::shared_ptr<const Aws::EC2::EC2Client> ec2_client)
    : ec2_client_(std::move(ec2_client)) {}

std::shared_ptr<Ec2BenchmarkResult> Ec2BenchmarkRunner::RunEc2Config(
    const std::shared_ptr<Ec2BenchmarkConfig>& config) {
  return std::dynamic_pointer_cast<Ec2BenchmarkResult>(RunConfig(config));
}

void Ec2BenchmarkRunner::Setup() {
  typed_config_ = std::dynamic_pointer_cast<Ec2BenchmarkConfig>(config_);
  Assert(typed_config_, "Ec2BenchmarkRunner can only consume Ec2BenchmarkConfigs.");

  run_instance_requests_.resize(typed_config_->repetition_count_);
  running_instances_.reserve(typed_config_->concurrent_invocation_count_);

  // Create RunInstancesRequests from current Ec2BenchmarkConfig.
  const std::vector<size_t> instances_per_request = [&]() {
    std::vector<size_t> result(typed_config_->concurrent_invocation_count_ / kMaxInstancesPerRequest,
                               kMaxInstancesPerRequest);

    if (const size_t remainder = typed_config_->concurrent_invocation_count_ % kMaxInstancesPerRequest) {
      result.push_back(remainder);
    }

    return result;
  }();

  for (size_t i = 0; i < typed_config_->repetition_count_; i++) {
    run_instance_requests_[i].reserve(instances_per_request.size());

    for (const auto& instance_count : instances_per_request) {
      run_instance_requests_[i].push_back(
          Aws::EC2::Model::RunInstancesRequest()
              .WithImageId(kAmazonLinux2ImageId)
              .WithTagSpecifications({Aws::EC2::Model::TagSpecification()
                                          .WithResourceType(Aws::EC2::Model::ResourceType::instance)
                                          .WithTags({Aws::EC2::Model::Tag().WithKey("Name").WithValue(
                                              typed_config_->instance_names_[i])})})
              .WithInstanceType(typed_config_->instance_type_)
              .WithMinCount(instance_count)
              .WithMaxCount(instance_count));
    }
  }
}

void Ec2BenchmarkRunner::Teardown() { run_instance_requests_.clear(); }

std::shared_ptr<AbstractBenchmarkResult> Ec2BenchmarkRunner::OnRunConfig() {
  result_ = std::make_shared<Ec2BenchmarkResult>(typed_config_->repetition_count_,
                                                 typed_config_->concurrent_invocation_count_);

  const auto benchmark_begin = std::chrono::steady_clock::now();

  for (size_t i = 0; i < typed_config_->repetition_count_; i++) {
    const auto repetition_begin = std::chrono::steady_clock::now();

    for (const auto& run_instance_request : run_instance_requests_[i]) {
      const auto outcome = ec2_client_->RunInstances(run_instance_request);
      Assert(outcome.IsSuccess(), outcome.GetError().GetMessage());

      const auto& run_instance_result = outcome.GetResult();

      for (const auto& instance : run_instance_result.GetInstances()) {
        DebugAssert(instance.GetState().GetName() == Aws::EC2::Model::InstanceStateName::pending,
                    instance.GetStateReason().GetMessage());
        running_instances_[instance.GetInstanceId()] = false;
      }
    }

    Aws::EC2::Model::DescribeInstancesRequest describe_instances_request;
    size_t running_instances_count = 0;

    while (running_instances_count < typed_config_->concurrent_invocation_count_) {
      const auto describe_instances_outcome = ec2_client_->DescribeInstances(describe_instances_request);
      Assert(describe_instances_outcome.IsSuccess(), describe_instances_outcome.GetError().GetMessage());

      const auto& describe_instances_result = describe_instances_outcome.GetResult();

      for (const auto& reservation : describe_instances_result.GetReservations()) {
        const auto& instances = reservation.GetInstances();

        const auto instance_running_end = std::chrono::steady_clock::now();
        const auto launch_instance_duration_ms =
            std::chrono::duration<double, std::milli>(instance_running_end - repetition_begin).count();

        for (const auto& instance : instances) {
          if (instance.GetState().GetName() != Aws::EC2::Model::InstanceStateName::running) {
            continue;
          }

          const Aws::String& instance_id = instance.GetInstanceId();

          if (running_instances_.find(instance_id) != running_instances_.cend()) {
            // Skip if we marked this instance as running before.
            if (running_instances_[instance_id]) {
              continue;
            }

            // Mark this instance as running.
            running_instances_[instance_id] = true;
            ++running_instances_count;
            result_->RegisterInstanceLaunch(i, instance_id, launch_instance_duration_ms);
          }
        }
      }

      if (!describe_instances_result.GetNextToken().empty()) {
        describe_instances_request.SetNextToken(describe_instances_result.GetNextToken());
      } else {
        describe_instances_request = Aws::EC2::Model::DescribeInstancesRequest();
      }
    }

    const auto repetition_end = std::chrono::steady_clock::now();
    const double repetition_duration_ms =
        std::chrono::duration<double, std::milli>(repetition_end - repetition_begin).count();

    TerminateInstances(repetition_duration_ms, i);
  }

  const auto benchmark_end = std::chrono::steady_clock::now();
  const double benchmark_duration_ms =
      std::chrono::duration<double, std::milli>(benchmark_end - benchmark_begin).count();
  result_->FinalizeResult(benchmark_duration_ms);

  return result_;
}

void Ec2BenchmarkRunner::TerminateInstances(const double repetition_duration_ms, const size_t repetition) {
  if (running_instances_.empty()) {
    return;
  }

  Aws::Vector<Aws::String> instance_id_vector;
  instance_id_vector.reserve(running_instances_.size());

  std::transform(running_instances_.cbegin(), running_instances_.cend(), std::back_inserter(instance_id_vector),
                 [](const auto& instance) { return instance.first; });

  const auto terminate_instances_outcome =
      ec2_client_->TerminateInstances(Aws::EC2::Model::TerminateInstancesRequest().WithInstanceIds(instance_id_vector));
  Assert(terminate_instances_outcome.IsSuccess(), terminate_instances_outcome.GetError().GetMessage());

  const auto instance_cooldown_begin = std::chrono::steady_clock::now();

  Aws::EC2::Model::DescribeInstancesRequest describe_instances_request;

  while (!running_instances_.empty()) {
    const auto describe_instances_outcome = ec2_client_->DescribeInstances(describe_instances_request);
    Assert(describe_instances_outcome.IsSuccess(), describe_instances_outcome.GetError().GetMessage());

    const auto instance_cooldown_end = std::chrono::steady_clock::now();
    const auto instance_cooldown_ms =
        std::chrono::duration<double, std::milli>(instance_cooldown_end - instance_cooldown_begin).count();

    const auto& describe_instances_result = describe_instances_outcome.GetResult();

    for (const auto& reservation : describe_instances_result.GetReservations()) {
      const auto& instances = reservation.GetInstances();

      for (const auto& instance : instances) {
        if (instance.GetState().GetName() != Aws::EC2::Model::InstanceStateName::terminated) {
          continue;
        }

        const Aws::String& instance_id = instance.GetInstanceId();

        if (!result_->ContainsLaunchDuration(repetition, instance_id)) {
          // Skip if the instance does not belong to our benchmark.
          continue;
        }

        // Only update cooldown if it was not updated before.
        if (!result_->LaunchDurationHasCooldown(repetition, instance_id)) {
          result_->UpdateCooldown(repetition, instance_id, instance_cooldown_ms);
          running_instances_.erase(instance_id);
        }
      }
    }

    if (!describe_instances_result.GetNextToken().empty()) {
      describe_instances_request.SetNextToken(describe_instances_result.GetNextToken());
    } else {
      describe_instances_request = Aws::EC2::Model::DescribeInstancesRequest();
    }
  }

  result_->FinalizeRepetition(repetition, repetition_duration_ms);
}

}  // namespace skyrise
