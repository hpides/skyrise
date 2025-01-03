#include "ec2_benchmark_runner.hpp"

#include <chrono>
#include <regex>
#include <thread>

#include <aws/core/utils/base64/Base64.h>
#include <aws/ec2/model/ArchitectureType.h>
#include <aws/ec2/model/DescribeInstanceTypesRequest.h>
#include <aws/ec2/model/DescribeInstanceTypesResponse.h>
#include <aws/ec2/model/DescribeInstancesRequest.h>
#include <aws/ec2/model/DescribeInstancesResponse.h>
#include <aws/ec2/model/StartInstancesRequest.h>
#include <aws/ec2/model/StopInstancesRequest.h>
#include <aws/ec2/model/TerminateInstancesRequest.h>
#include <aws/ssm/model/GetParametersRequest.h>
#include <boost/algorithm/string/predicate.hpp>
#include <libssh/libssh.h>
#include <magic_enum/magic_enum.hpp>

#include "abstract_benchmark_config.hpp"
#include "utils/assert.hpp"

namespace skyrise {

namespace {

const std::regex kArchitectureTypeRegex("<architecture_type>");
const Aws::String kLatestImagePath{"/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-6.1-<architecture_type>"};
// AWS recommends to only run 100 EC2 instances per request:
// https://sdk.amazonaws.com/cpp/api/LATEST/class_aws_1_1_e_c2_1_1_e_c2_client.html#aac0e6842f8753e05bd6c2f5ae4bf9e94
const size_t kMaxInstancesPerRequest = 1000;  // Stop-gap solution.
const long kSshTimeoutMs = 1000;

}  // namespace

Ec2BenchmarkRunner::Ec2BenchmarkRunner(std::shared_ptr<const Aws::EC2::EC2Client> ec2_client,
                                       std::shared_ptr<const Aws::SSM::SSMClient> ssm_client, const bool metering,
                                       const bool introspection)
    : AbstractBenchmarkRunner(metering, introspection),
      ec2_client_(std::move(ec2_client)),
      ssm_client_(std::move(ssm_client)) {}

std::shared_ptr<Ec2BenchmarkResult> Ec2BenchmarkRunner::RunEc2Config(
    const std::shared_ptr<Ec2BenchmarkConfig>& config) {
  return std::dynamic_pointer_cast<Ec2BenchmarkResult>(RunConfig(config));
}

Aws::String Ec2BenchmarkRunner::GetLatestAmi(Aws::EC2::Model::InstanceType instance_type) {
  // (1) Retrieve supported architecture.
  const auto describe_instance_types_request = Aws::EC2::Model::DescribeInstanceTypesRequest().WithInstanceTypes(
      Aws::Vector<Aws::EC2::Model::InstanceType>{instance_type});

  const auto describe_instance_types_response = ec2_client_->DescribeInstanceTypes(describe_instance_types_request);

  const auto check_describe_instance_types_response = [&describe_instance_types_response]() {
    if (describe_instance_types_response.IsSuccess()) {
      const auto& result = describe_instance_types_response.GetResult();

      if (result.GetInstanceTypes().size() == 1 && result.GetInstanceTypes()[0].ProcessorInfoHasBeenSet()) {
        const auto processor_info = result.GetInstanceTypes()[0].GetProcessorInfo();

        return processor_info.SupportedArchitecturesHasBeenSet() &&
               processor_info.GetSupportedArchitectures().size() == 1;
      }
    }
    return false;
  };

  Assert(check_describe_instance_types_response(), "Retrieval of supported architecture failed.");

  const auto instance_type_response = describe_instance_types_response.GetResult().GetInstanceTypes()[0];
  const auto supported_architecture_type = instance_type_response.GetProcessorInfo().GetSupportedArchitectures()[0];

  const auto architecture_type_name =
      Aws::EC2::Model::ArchitectureTypeMapper::GetNameForArchitectureType(supported_architecture_type);

  // (2) Retrieve latest image ID.
  const auto image_path = std::regex_replace(kLatestImagePath, kArchitectureTypeRegex, architecture_type_name);
  const auto latest_ami_request =
      Aws::SSM::Model::GetParametersRequest().WithNames(Aws::Vector<Aws::String>{image_path});
  const auto get_parameters_response = ssm_client_->GetParameters(latest_ami_request);

  const auto check_get_parameters_response = [&get_parameters_response]() {
    if (get_parameters_response.IsSuccess()) {
      const auto parameters = get_parameters_response.GetResult().GetParameters();

      return parameters.size() == 1 && parameters[0].ValueHasBeenSet();
    }
    return false;
  };

  Assert(check_get_parameters_response(), "Retrieval of latest image ID failed.");

  return get_parameters_response.GetResult().GetParameters()[0].GetValue();
}

void Ec2BenchmarkRunner::Setup() {
  typed_config_ = std::dynamic_pointer_cast<Ec2BenchmarkConfig>(config_);
  Assert(typed_config_, "Ec2BenchmarkRunner can only consume Ec2BenchmarkConfigs.");

  const auto latest_image_id = GetLatestAmi(typed_config_->GetInstanceType());

  run_instances_requests_.resize(typed_config_->GetRepetitionCount());
  running_instances_.reserve(typed_config_->GetConcurrentInstanceCount());

  // TODO(tobodner): Deprecate this. We will always start 1 instance per request.
  // Create RunInstancesRequests from current Ec2BenchmarkConfig.
  const std::vector<size_t> instances_per_request = [&]() {
    std::vector<size_t> result(typed_config_->GetConcurrentInstanceCount() / kMaxInstancesPerRequest,
                               kMaxInstancesPerRequest);

    if (const size_t remainder = typed_config_->GetConcurrentInstanceCount() % kMaxInstancesPerRequest) {
      result.push_back(remainder);
    }

    return result;
  }();

  for (size_t i = 0; i < typed_config_->GetRepetitionCount(); ++i) {
    run_instances_requests_[i].reserve(typed_config_->GetConcurrentInstanceCount());

    for (const auto& instance_count : instances_per_request) {
      for (size_t j = 0; j < instance_count; ++j) {
        std::string user_data = typed_config_->GetUserData()(i, j);

        Assert(boost::algorithm::starts_with(user_data, kHashbang),
               "Invalid user data. User data must begin with " + kHashbang);

        if (!boost::algorithm::ends_with(user_data, kDefaultUserData)) {
          user_data += " && " + kDefaultUserData;
        }

        // Base64 encoding of user-data is required.
        Aws::Utils::ByteBuffer user_data_buffer(user_data.size());
        std::copy(user_data.begin(), user_data.end(), user_data_buffer.GetUnderlyingData());

        auto run_instances_request =
            Aws::EC2::Model::RunInstancesRequest()
                .WithKeyName("skyrise-ci")
                .WithInstanceType(typed_config_->GetInstanceType())
                .WithImageId(latest_image_id)
                .WithTagSpecifications({Aws::EC2::Model::TagSpecification()
                                            .WithResourceType(Aws::EC2::Model::ResourceType::instance)
                                            .WithTags({Aws::EC2::Model::Tag().WithKey("Name").WithValue(
                                                typed_config_->GetInstanceNames()[i])})})
                .WithMinCount(1)
                .WithMaxCount(1)
                .WithUserData(Aws::Utils::Base64::Base64().Encode(user_data_buffer))
                .WithInstanceInitiatedShutdownBehavior(Aws::EC2::Model::ShutdownBehavior::terminate)
                .WithPlacement(Aws::EC2::Model::Placement().WithAvailabilityZone(kDefaultAvailabilityZone));

        if (typed_config_->IsVpcEnabled()) {
          Aws::EC2::Model::InstanceNetworkInterfaceSpecification network_interface;
          network_interface.SetSubnetId(kDefaultSubnet);
          network_interface.SetDeviceIndex(0);
          Aws::Vector<Aws::String> security_groups;
          security_groups.push_back(kDefaultSecurityGroup);
          security_groups.push_back(kSshSecurityGroup);
          network_interface.SetGroups(security_groups);
          run_instances_request.AddNetworkInterfaces(network_interface);
        } else {
          run_instances_request.SetSecurityGroupIds(Aws::Vector<Aws::String>{kDefaultSecurityGroup, kSshSecurityGroup});
        }

        run_instances_requests_[i].push_back(run_instances_request);
      }
    }
  }
}

void Ec2BenchmarkRunner::Teardown() {
  // TODO(tobodner): Consider to terminate any remaining instances here. For example, in case the benchmark crashed.
  run_instances_requests_.clear();
}

std::shared_ptr<AbstractBenchmarkResult> Ec2BenchmarkRunner::OnRunConfig() {
  result_ = std::make_shared<Ec2BenchmarkResult>(typed_config_->GetInstanceType(),
                                                 typed_config_->GetConcurrentInstanceCount(),
                                                 typed_config_->GetRepetitionCount());

  const auto benchmark_begin = std::chrono::steady_clock::now();

  for (size_t i = 0; i < typed_config_->GetRepetitionCount(); ++i) {
    const auto repetition_begin = std::chrono::steady_clock::now();

    ColdstartInstances(i, repetition_begin);

    if (typed_config_->GetWarmupType() == Ec2WarmupType::kDefault) {
      StopInstances(i);
      WarmstartInstances(i);
    }

    Aws::Utils::Json::JsonValue invocation_results = typed_config_->GetBenchmark()(i);
    result_->SetInvocationResults(invocation_results, i);

    TerminateInstances(i);

    const auto repetition_end = std::chrono::steady_clock::now();
    const double repetition_duration_ms =
        std::chrono::duration<double, std::milli>(repetition_end - repetition_begin).count();
    result_->FinalizeRepetition(i, repetition_duration_ms);
  }

  const auto benchmark_end = std::chrono::steady_clock::now();
  const double benchmark_duration_ms =
      std::chrono::duration<double, std::milli>(benchmark_end - benchmark_begin).count();
  result_->FinalizeResult(benchmark_duration_ms);

  return result_;
}

void Ec2BenchmarkRunner::ColdstartInstances(
    size_t repetition, const std::chrono::time_point<std::chrono::steady_clock>& repetition_begin) {
  for (const auto& run_instances_request : run_instances_requests_[repetition]) {
    std::this_thread::sleep_for(std::chrono::milliseconds(kEc2RequestIntervalMilliseconds));
    const auto outcome = ec2_client_->RunInstances(run_instances_request);
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

  while (running_instances_count < typed_config_->GetConcurrentInstanceCount()) {
    const auto describe_instances_outcome = ec2_client_->DescribeInstances(describe_instances_request);
    Assert(describe_instances_outcome.IsSuccess(), describe_instances_outcome.GetError().GetMessage());

    const auto& describe_instances_result = describe_instances_outcome.GetResult();

    for (const auto& reservation : describe_instances_result.GetReservations()) {
      const auto& instances = reservation.GetInstances();

      for (const auto& instance : instances) {
        if (instance.GetState().GetName() != Aws::EC2::Model::InstanceStateName::running ||
            !instance.PublicIpAddressHasBeenSet()) {
          continue;
        }
        const Aws::String& instance_id = instance.GetInstanceId();

        if (running_instances_.find(instance_id) != running_instances_.cend()) {
          result_->RegisterInstanceBillingStarts(repetition, instance_id, std::chrono::steady_clock::now(), false);
        }

        if (!IsSshActive(instance.GetPublicIpAddress())) {
          continue;
        }
        const auto instance_running_end = std::chrono::steady_clock::now();

        if (running_instances_.find(instance_id) != running_instances_.cend()) {
          // Skip if we marked this instance as running before.
          if (running_instances_[instance_id]) {
            continue;
          }

          // Mark this instance as running.
          running_instances_[instance_id] = true;
          ++running_instances_count;
          const auto instance_coldstart_duration_ms =
              std::chrono::duration<double, std::milli>(instance_running_end - repetition_begin).count();
          result_->RegisterInstanceColdstart(repetition, instance_id, instance_coldstart_duration_ms);
        }
      }
    }

    if (!describe_instances_result.GetNextToken().empty()) {
      describe_instances_request.SetNextToken(describe_instances_result.GetNextToken());
    } else {
      describe_instances_request = Aws::EC2::Model::DescribeInstancesRequest();
    }
  }
}

void Ec2BenchmarkRunner::StopInstances(const size_t repetition) {
  if (running_instances_.empty()) {
    return;
  }
  Aws::Vector<Aws::String> instance_ids = GetRunningInstanceIds();

  const auto stop_instances_request = Aws::EC2::Model::StopInstancesRequest().WithInstanceIds(instance_ids);
  const auto stop_instances_outcome = ec2_client_->StopInstances(stop_instances_request);
  Assert(stop_instances_outcome.IsSuccess(), stop_instances_outcome.GetError().GetMessage());

  const auto instance_stop_begin = std::chrono::steady_clock::now();

  for (const auto& instance_id : instance_ids) {
    result_->RegisterInstanceBillingStops(repetition, instance_id, instance_stop_begin);
  }

  Aws::EC2::Model::DescribeInstancesRequest describe_instances_request;

  while (HasRunningInstances()) {
    const auto describe_instances_outcome = ec2_client_->DescribeInstances(describe_instances_request);
    Assert(describe_instances_outcome.IsSuccess(), describe_instances_outcome.GetError().GetMessage());

    const auto instance_stop_end = std::chrono::steady_clock::now();

    const auto& describe_instances_result = describe_instances_outcome.GetResult();

    for (const auto& reservation : describe_instances_result.GetReservations()) {
      const auto& instances = reservation.GetInstances();

      for (const auto& instance : instances) {
        if (instance.GetState().GetName() != Aws::EC2::Model::InstanceStateName::stopped) {
          continue;
        }

        const Aws::String& instance_id = instance.GetInstanceId();

        if (!result_->ContainsInstanceColdstartDuration(repetition, instance_id)) {
          // Skip if the instance does not belong to our benchmark.
          continue;
        }

        // Only register stop duration if it has not been registered earlier.
        if (!result_->ContainsInstanceStopDuration(repetition, instance_id)) {
          const auto instance_stop_duration_ms =
              std::chrono::duration<double, std::milli>(instance_stop_end - instance_stop_begin).count();
          result_->RegisterInstanceStop(repetition, instance_id, instance_stop_duration_ms);
          running_instances_[instance_id] = false;
        }
      }
    }
    if (!describe_instances_result.GetNextToken().empty()) {
      describe_instances_request.SetNextToken(describe_instances_result.GetNextToken());
    } else {
      describe_instances_request = Aws::EC2::Model::DescribeInstancesRequest();
    }
  }
}

void Ec2BenchmarkRunner::WarmstartInstances(const size_t repetition) {
  if (running_instances_.empty()) {
    return;
  }

  Aws::Vector<Aws::String> instance_ids = GetRunningInstanceIds();

  const auto warmstart_instances_request = Aws::EC2::Model::StartInstancesRequest().WithInstanceIds(instance_ids);
  const auto warmstart_instances_outcome = ec2_client_->StartInstances(warmstart_instances_request);
  Assert(warmstart_instances_outcome.IsSuccess(), warmstart_instances_outcome.GetError().GetMessage());

  const auto instance_warmstart_begin = std::chrono::steady_clock::now();

  Aws::EC2::Model::DescribeInstancesRequest describe_instances_request;

  while (HasStoppedInstances()) {
    const auto describe_instances_outcome = ec2_client_->DescribeInstances(describe_instances_request);
    Assert(describe_instances_outcome.IsSuccess(), describe_instances_outcome.GetError().GetMessage());

    const auto& describe_instances_result = describe_instances_outcome.GetResult();

    for (const auto& reservation : describe_instances_result.GetReservations()) {
      const auto& instances = reservation.GetInstances();

      for (const auto& instance : instances) {
        if (instance.GetState().GetName() != Aws::EC2::Model::InstanceStateName::running ||
            !instance.PublicIpAddressHasBeenSet()) {
          continue;
        }
        const Aws::String& instance_id = instance.GetInstanceId();

        if (running_instances_.find(instance_id) != running_instances_.cend()) {
          result_->RegisterInstanceBillingStarts(repetition, instance_id, std::chrono::steady_clock::now(), true);
        }

        if (!IsSshActive(instance.GetPublicIpAddress())) {
          continue;
        }
        const auto instance_warmstart_end = std::chrono::steady_clock::now();

        if (!result_->ContainsInstanceColdstartDuration(repetition, instance_id)) {
          // Skip if the instance does not belong to our benchmark.
          continue;
        }

        // Only register warmstart duration if it has not been registered earlier.
        if (!result_->ContainsInstanceWarmstartDuration(repetition, instance_id)) {
          const auto instance_warmstart_duration_ms =
              std::chrono::duration<double, std::milli>(instance_warmstart_end - instance_warmstart_begin).count();
          result_->RegisterInstanceWarmstart(repetition, instance_id, instance_warmstart_duration_ms);
          running_instances_[instance_id] = true;
        }
      }
    }
    if (!describe_instances_result.GetNextToken().empty()) {
      describe_instances_request.SetNextToken(describe_instances_result.GetNextToken());
    } else {
      describe_instances_request = Aws::EC2::Model::DescribeInstancesRequest();
    }
  }
}

void Ec2BenchmarkRunner::TerminateInstances(const size_t repetition) {
  if (running_instances_.empty()) {
    return;
  }

  Aws::Vector<Aws::String> instance_ids = GetRunningInstanceIds();

  const auto terminate_instances_outcome =
      ec2_client_->TerminateInstances(Aws::EC2::Model::TerminateInstancesRequest().WithInstanceIds(instance_ids));
  Assert(terminate_instances_outcome.IsSuccess(), terminate_instances_outcome.GetError().GetMessage());

  const auto instance_termination_begin = std::chrono::steady_clock::now();

  for (const auto& instance_id : instance_ids) {
    result_->RegisterInstanceBillingStops(repetition, instance_id, instance_termination_begin);
  }

  Aws::EC2::Model::DescribeInstancesRequest describe_instances_request;

  while (!running_instances_.empty()) {
    const auto describe_instances_outcome = ec2_client_->DescribeInstances(describe_instances_request);
    Assert(describe_instances_outcome.IsSuccess(), describe_instances_outcome.GetError().GetMessage());

    const auto instance_termination_end = std::chrono::steady_clock::now();

    const auto& describe_instances_result = describe_instances_outcome.GetResult();

    for (const auto& reservation : describe_instances_result.GetReservations()) {
      const auto& instances = reservation.GetInstances();

      for (const auto& instance : instances) {
        if (instance.GetState().GetName() != Aws::EC2::Model::InstanceStateName::terminated) {
          continue;
        }

        const Aws::String& instance_id = instance.GetInstanceId();

        if (!result_->ContainsInstanceColdstartDuration(repetition, instance_id)) {
          // Skip if the instance does not belong to our benchmark.
          continue;
        }

        // Only register termination duration if it has not been registered earlier.
        if (!result_->ContainsInstanceTerminationDuration(repetition, instance_id)) {
          const auto instance_termination_duration_ms =
              std::chrono::duration<double, std::milli>(instance_termination_end - instance_termination_begin).count();
          result_->RegisterInstanceTermination(repetition, instance_id, instance_termination_duration_ms);
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
}

bool Ec2BenchmarkRunner::IsSshActive(const std::string& hostname) {
  ssh_session ssh_session = ssh_new();
  if (ssh_session == nullptr) {
    Fail("SSH session cannot be created.");
  }

  const long timeout_us = kSshTimeoutMs * 1000;
  ssh_options_set(ssh_session, SSH_OPTIONS_USER, "user");  // required option
  ssh_options_set(ssh_session, SSH_OPTIONS_HOST, hostname.c_str());
  ssh_options_set(ssh_session, SSH_OPTIONS_TIMEOUT_USEC, &timeout_us);

  int status = ssh_connect(ssh_session);

  ssh_disconnect(ssh_session);
  ssh_free(ssh_session);

  return status == SSH_OK;
}

Aws::Vector<Aws::String> Ec2BenchmarkRunner::GetRunningInstanceIds() {
  Aws::Vector<Aws::String> instance_ids;
  instance_ids.reserve(running_instances_.size());

  std::transform(running_instances_.cbegin(), running_instances_.cend(), std::back_inserter(instance_ids),
                 [](const auto& instance) { return instance.first; });
  return instance_ids;
}

bool Ec2BenchmarkRunner::HasRunningInstances() {
  return std::any_of(running_instances_.begin(), running_instances_.end(),
                     [](const auto& instance) { return instance.second; });
}

bool Ec2BenchmarkRunner::HasStoppedInstances() {
  return std::any_of(running_instances_.begin(), running_instances_.end(),
                     [](const auto& instance) { return !instance.second; });
}

}  // namespace skyrise
