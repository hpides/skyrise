#include "lambda_network_benchmark.hpp"

#include <algorithm>
#include <cmath>
#include <numeric>
#include <thread>

#include <aws/core/utils/base64/Base64.h>
#include <aws/ec2/model/DescribeInstanceStatusRequest.h>
#include <aws/ec2/model/DescribeInstancesRequest.h>
#include <aws/ec2/model/RunInstancesRequest.h>
#include <aws/ec2/model/TerminateInstancesRequest.h>
#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/sqs/model/DeleteQueueRequest.h>

#include "benchmark_result_aggregate.hpp"
#include "lambda_benchmark_output.hpp"

namespace skyrise {

namespace {

const Aws::String kName = "lambdaNetworkBenchmark";
// TODO(tobodner): Extract Ec2BenchmarkRunner::GetLatestAmi and use it here.
const Aws::String kAmazonLinux2023ImageIdArm = "ami-095f54910906baa61";
const Aws::String kIperfInboundSecurityGroupId = "sg-05d0e584fb835e232";  // Allows all incoming TCP connections.
constexpr size_t kIperfServerDefaultPort = 5201;
constexpr size_t kInstanceStatusPollIntervalSeconds = 10;
constexpr size_t kMaximumFunctionsPerInstance = 10;
constexpr auto kFunctionName = "skyriseNetworkIoFunction";
const auto kExpectedInvocationResponseTemplate =
    Aws::Utils::Json::JsonValue()
        .WithArray("intervals_upload", Aws::Utils::Array<Aws::Utils::Json::JsonValue>(0))
        .WithArray("intervals_download", Aws::Utils::Array<Aws::Utils::Json::JsonValue>(0))
        .WithString("environment_id", "")
        .WithString("ip", "")
        .WithString("invocation_start_timestamp", "")
        .WithString("measurement_upload_start_timestamp", "")
        .WithString("measurement_download_start_timestamp", "");

}  // namespace

LambdaNetworkBenchmark::LambdaNetworkBenchmark(
    std::shared_ptr<const CostCalculator> cost_calculator, const std::shared_ptr<const Aws::EC2::EC2Client>& ec2_client,
    const std::shared_ptr<const Aws::SQS::SQSClient>& sqs_client, const std::vector<size_t>& function_instance_sizes_mb,
    const std::vector<size_t>& concurrent_instance_counts, const size_t repetition_count, const size_t duration_s,
    const size_t report_interval_ms, const size_t target_throughput_mbps, const bool enable_download,
    const bool enable_upload, const DistinctFunctionPerRepetition distinct_function_per_repetition,
    const bool enable_vpc, const int message_size_kb)
    : LambdaBenchmark(std::move(cost_calculator)),
      ec2_client_(ec2_client),
      sqs_client_(sqs_client),
      ec2_instance_types_(ResolveEc2InstanceTypes(function_instance_sizes_mb, concurrent_instance_counts)),
      duration_s_(duration_s),
      report_interval_ms_(report_interval_ms),
      target_throughput_mbps_(target_throughput_mbps),
      enable_download_(enable_download),
      enable_upload_(enable_upload),
      enable_vpc_(enable_vpc),
      message_size_kb_(message_size_kb) {
  Assert(enable_download_ || enable_upload_, "Either upload, download, or both must be enabled.");
  std::vector<std::function<void()>> after_repetition_callbacks;
  after_repetition_callbacks.reserve(repetition_count);

  for (size_t i = 0; i < repetition_count; ++i) {
    after_repetition_callbacks.emplace_back(
        [&]() { std::this_thread::sleep_for(std::chrono::seconds(kLambdaNetworkTokenBucketRefillSeconds)); });
  }

  for (const auto& function_instance_size_mb : function_instance_sizes_mb) {
    for (const auto& concurrent_instance_count : concurrent_instance_counts) {
      const auto benchmark_config = std::make_shared<LambdaBenchmarkConfig>(
          kFunctionName, "", Warmup::kNo, distinct_function_per_repetition, EventQueue::kNo, false,
          function_instance_size_mb, "", concurrent_instance_count, repetition_count, after_repetition_callbacks,
          enable_vpc_);
      configs_.emplace_back(LambdaBenchmarkParameters{.function_instance_size_mb = function_instance_size_mb,
                                                      .concurrent_instance_count = concurrent_instance_count,
                                                      .repetition_count = repetition_count},
                            benchmark_config);
    }
  }
}

const Aws::String& LambdaNetworkBenchmark::Name() const { return kName; }

Aws::Utils::Array<Aws::Utils::Json::JsonValue> LambdaNetworkBenchmark::OnRun(
    const std::shared_ptr<LambdaBenchmarkRunner>& runner) {
  std::vector<std::shared_ptr<LambdaBenchmarkResult>> results;
  results.reserve(configs_.size());

  for (size_t i = 0; i < configs_.size(); ++i) {
    Setup(ec2_instance_types_[i], ec2_cluster_size_[i], configs_[i].second->GetConcurrentInstanceCount(),
          configs_[i].second->GetRepetitionCount());
    configs_[i].second->SetPayloads(GeneratePayloads(configs_[i].second));
    results.push_back(runner->RunLambdaConfig(configs_[i].second));
    results.back()->ValidateInvocationResults(kExpectedInvocationResponseTemplate);

    Teardown();
  }

  Aws::Utils::Array<Aws::Utils::Json::JsonValue> results_array(results.size());

  for (size_t i = 0; i < results.size(); ++i) {
    results_array[i] = GenerateResultOutput(results[i], configs_[i], ec2_runtimes_[i], ec2_instance_types_[i]);
  }

  return results_array;
}

Aws::Utils::Json::JsonValue LambdaNetworkBenchmark::GenerateResultOutput(
    const std::shared_ptr<LambdaBenchmarkResult>& benchmark_result,
    const std::pair<LambdaBenchmarkParameters, std::shared_ptr<LambdaBenchmarkConfig>>& config,
    const std::vector<std::chrono::milliseconds>& ec2_runtimes,
    const Aws::EC2::Model::InstanceType ec2_instance_type) const {
  const auto& benchmark_repetitions = benchmark_result->GetRepetitionResults();
  std::vector<double> intervals_upload_vector;
  std::vector<double> intervals_download_vector;

  for (const auto& benchmark_repetition : benchmark_repetitions) {
    for (const auto& invoke_result : benchmark_repetition.GetInvocationResults()) {
      const auto intervals_upload = invoke_result.GetResponseBody().GetArray("intervals_upload");
      for (size_t i = 0; i < intervals_upload.GetLength(); ++i) {
        intervals_upload_vector.push_back(static_cast<double>(intervals_upload[i].AsInt64()));
      }
      const auto intervals_download = invoke_result.GetResponseBody().GetArray("intervals_download");
      for (size_t i = 0; i < intervals_download.GetLength(); ++i) {
        intervals_download_vector.push_back(static_cast<double>(intervals_download[i].AsInt64()));
      }
    }
  }
  if (intervals_download_vector.empty()) {
    intervals_download_vector.push_back(-1);
  }
  if (intervals_upload_vector.empty()) {
    intervals_upload_vector.push_back(-1);
  }
  const BenchmarkResultAggregate intervals_upload_aggregate(intervals_upload_vector);
  const BenchmarkResultAggregate intervals_download_aggregate(intervals_download_vector);

  // Aggregate the runtimes from the EC2 cluster.
  const std::chrono::milliseconds ec2_compute_duration_ms =
      std::accumulate(ec2_runtimes.begin(), ec2_runtimes.end(), static_cast<std::chrono::milliseconds>(0));

  const auto ec2_cost = cost_calculator_->CalculateCostEc2(ec2_compute_duration_ms.count(), ec2_instance_type);
  auto benchmark_output = LambdaBenchmarkOutput(Name(), config.first, benchmark_result);

  return benchmark_output.WithInt64Argument("function_instance_size_mb", config.first.function_instance_size_mb)
      .WithInt64Argument("concurrent_instance_count", config.first.concurrent_instance_count)
      .WithInt64Argument("repetition_count", config.first.repetition_count)
      .WithStringArgument("ec2_instance_type",
                          Aws::EC2::Model::InstanceTypeMapper::GetNameForInstanceType(ec2_instance_type))
      .WithInt64Argument("report_interval_ms", report_interval_ms_)
      .WithInt64Argument("duration_s", duration_s_)
      .WithStringArgument("target_throughput_mbps", target_throughput_mbps_ == std::numeric_limits<size_t>::max()
                                                        ? "unlimited"
                                                        : std::to_string(target_throughput_mbps_))
      .WithBoolArgument("enable_vpc", enable_vpc_)
      .WithInt64Argument("message_size_kb", message_size_kb_)

      // TODO(tobodner): The total duration should be calculated and set consolidated for all benchmarks.
      .WithDoubleMetric("total_duration_ms", ec2_compute_duration_ms.count() + benchmark_result->GetDurationMs())
      .WithDoubleMetric("total_costs_usd", benchmark_output.GetLambdaRuntimeCosts() + ec2_cost)
      .WithDoubleMetric("ec2_runtime_duration_ms", ec2_compute_duration_ms.count())
      .WithDoubleMetric("ec2_cost_usd", ec2_cost)
      .WithDoubleMetric("throughput_upload_mbps_minimum", intervals_upload_aggregate.GetMinimum())
      .WithDoubleMetric("throughput_upload_mbps_maximum", intervals_upload_aggregate.GetMaximum())
      .WithDoubleMetric("throughput_upload_mbps_average", intervals_upload_aggregate.GetAverage())
      .WithDoubleMetric("throughput_upload_mbps_median", intervals_upload_aggregate.GetMedian())
      .WithDoubleMetric("throughput_upload_mbps_percentile_10", intervals_upload_aggregate.GetPercentile(10))
      .WithDoubleMetric("throughput_upload_mbps_percentile_1", intervals_upload_aggregate.GetPercentile(1))
      .WithDoubleMetric("throughput_upload_mbps_0.1", intervals_upload_aggregate.GetPercentile(0.1))
      .WithDoubleMetric("throughput_upload_mbps_0.01", intervals_upload_aggregate.GetPercentile(0.01))
      .WithDoubleMetric("throughput_upload_mbps_std_dev", intervals_upload_aggregate.GetStandardDeviation())
      .WithDoubleMetric("throughput_download_mbps_minimum", intervals_download_aggregate.GetMinimum())
      .WithDoubleMetric("throughput_download_mbps_maximum", intervals_download_aggregate.GetMaximum())
      .WithDoubleMetric("throughput_download_mbps_average", intervals_download_aggregate.GetAverage())
      .WithDoubleMetric("throughput_download_mbps_median", intervals_download_aggregate.GetMedian())
      .WithDoubleMetric("throughput_download_mbps_percentile_10", intervals_download_aggregate.GetPercentile(10))
      .WithDoubleMetric("throughput_download_mbps_percentile_1", intervals_download_aggregate.GetPercentile(1))
      .WithDoubleMetric("throughput_download_mbps_0.1", intervals_download_aggregate.GetPercentile(0.1))
      .WithDoubleMetric("throughput_download_mbps_0.01", intervals_download_aggregate.GetPercentile(0.01))
      .WithDoubleMetric("throughput_download_mbps_std_dev", intervals_download_aggregate.GetStandardDeviation())
      .WithObjectRepetitionMetric([&](const LambdaBenchmarkRepetitionResult& repetition_result) {
        std::vector<double> repetition_vector(
            repetition_result.GetInvocationResults().front().GetResponseBody().GetArray("intervals_upload").GetLength(),
            0);

        for (const auto& invoke_result : repetition_result.GetInvocationResults()) {
          const auto intervals = invoke_result.GetResponseBody().GetArray("intervals_upload");
          for (size_t i = 0; i < intervals.GetLength(); ++i) {
            repetition_vector[i] += static_cast<double>(intervals[i].AsInt64());
          }
        }

        Aws::Utils::Array<Aws::Utils::Json::JsonValue> repetition_json(repetition_vector.size());
        for (size_t i = 0; i < repetition_vector.size(); ++i) {
          repetition_json[i] = Aws::Utils::Json::JsonValue().AsInt64(repetition_vector[i]);
        }

        return std::make_tuple("aggregated_throughput_upload_mbps",
                               Aws::Utils::Json::JsonValue().AsArray(repetition_json));
      })
      .WithObjectRepetitionMetric([&](const LambdaBenchmarkRepetitionResult& repetition_result) {
        std::vector<double> repetition_vector(repetition_result.GetInvocationResults()
                                                  .front()
                                                  .GetResponseBody()
                                                  .GetArray("intervals_download")
                                                  .GetLength(),
                                              0);

        for (const auto& invoke_result : repetition_result.GetInvocationResults()) {
          const auto intervals = invoke_result.GetResponseBody().GetArray("intervals_download");
          for (size_t i = 0; i < intervals.GetLength(); ++i) {
            repetition_vector[i] += static_cast<double>(intervals[i].AsInt64());
          }
        }

        Aws::Utils::Array<Aws::Utils::Json::JsonValue> repetition_json(repetition_vector.size());
        for (size_t i = 0; i < repetition_vector.size(); ++i) {
          repetition_json[i] = Aws::Utils::Json::JsonValue().AsInt64(repetition_vector[i]);
        }

        return std::make_tuple("aggregated_throughput_download_mbps",
                               Aws::Utils::Json::JsonValue().AsArray(repetition_json));
      })
      .WithObjectInvocationMetric([&](const LambdaInvocationResult& invoke_result) {
        const auto intervals = invoke_result.GetResponseBody().GetArray("intervals_upload");
        Aws::Utils::Array<Aws::Utils::Json::JsonValue> intervals_json(intervals.GetLength());

        for (size_t i = 0; i < intervals.GetLength(); ++i) {
          intervals_json[i] = Aws::Utils::Json::JsonValue().AsInt64(intervals[i].AsInt64());
        }
        return std::make_tuple("intervals_upload", Aws::Utils::Json::JsonValue().AsArray(intervals_json));
      })
      .WithObjectInvocationMetric([&](const LambdaInvocationResult& invoke_result) {
        const auto intervals = invoke_result.GetResponseBody().GetArray("intervals_download");
        Aws::Utils::Array<Aws::Utils::Json::JsonValue> intervals_json(intervals.GetLength());

        for (size_t i = 0; i < intervals.GetLength(); ++i) {
          intervals_json[i] = Aws::Utils::Json::JsonValue().AsInt64(intervals[i].AsInt64());
        }
        return std::make_tuple("intervals_download", Aws::Utils::Json::JsonValue().AsArray(intervals_json));
      })
      .WithStringInvocationMetric([&](const LambdaInvocationResult& invoke_result) {
        return std::make_tuple("ip", invoke_result.GetResponseBody().GetString("ip"));
      })
      .WithStringInvocationMetric([&](const LambdaInvocationResult& invocation_result) {
        return std::make_tuple("invocation_start_timestamp",
                               invocation_result.GetResponseBody().GetString("invocation_start_timestamp"));
      })
      .WithStringInvocationMetric([&](const LambdaInvocationResult& invocation_result) {
        return std::make_tuple("measurement_upload_start_timestamp",
                               invocation_result.GetResponseBody().GetString("measurement_upload_start_timestamp"));
      })
      .WithStringInvocationMetric([&](const LambdaInvocationResult& invocation_result) {
        return std::make_tuple("measurement_download_start_timestamp",
                               invocation_result.GetResponseBody().GetString("measurement_download_start_timestamp"));
      })
      .Build();
}

std::vector<std::shared_ptr<Aws::IOStream>> LambdaNetworkBenchmark::GeneratePayloads(
    const std::shared_ptr<LambdaBenchmarkConfig>& config) {
  std::vector<std::shared_ptr<Aws::IOStream>> payloads;
  payloads.reserve(config->GetConcurrentInstanceCount());
  const std::string sqs_queue_url = SetupSqsQueue();
  sqs_queue_urls_.push_back(sqs_queue_url);

  for (const auto& [public_ip, id_and_iperf_server_count] : ec2_instance_map_) {
    size_t port = kIperfServerDefaultPort;

    for (size_t i = 0; i < id_and_iperf_server_count.first.second; ++i) {
      const auto payload_value = Aws::Utils::Json::JsonValue()
                                     // If we use a VPC, we utilize the private IP - otherwise the public IP.
                                     .WithString("ip", enable_vpc_ ? id_and_iperf_server_count.second : public_ip)
                                     .WithInt64("port", port)
                                     .WithInt64("duration_s", duration_s_)
                                     .WithDouble("report_interval_ms", static_cast<double>(report_interval_ms_))
                                     .WithInt64("target_throughput_mbps", target_throughput_mbps_)
                                     .WithBool("enable_download", enable_download_)
                                     .WithBool("enable_upload", enable_upload_)
                                     .WithString("sqs_queue_url", sqs_queue_url)
                                     .WithInteger("concurrent_instance_count", config->GetConcurrentInstanceCount())
                                     .WithInteger("message_size_kb", message_size_kb_);
      payloads.emplace_back(std::make_shared<Aws::StringStream>(payload_value.View().WriteCompact()));
      port++;
    }
  }

  return payloads;
}

void LambdaNetworkBenchmark::Setup(const Aws::EC2::Model::InstanceType ec2_instance_type, const size_t ec2_cluster_size,
                                   const size_t concurrent_instance_count, const size_t repetition_count) {
  std::vector<std::future<bool>> ec2_startup_futures;
  ec2_startup_futures.reserve(ec2_cluster_size);

  for (size_t i = 0; i < ec2_cluster_size; ++i) {
    ec2_startup_futures.emplace_back(std::async([concurrent_instance_count, ec2_cluster_size, i, repetition_count,
                                                 ec2_instance_type, this]() {
      size_t iperf_server_process_count =
          std::min(static_cast<size_t>(kMaximumFunctionsPerInstance), concurrent_instance_count);
      if (ec2_cluster_size > 1 && i == ec2_cluster_size - 1) {
        iperf_server_process_count = (concurrent_instance_count % kMaximumFunctionsPerInstance) == 0
                                         ? kMaximumFunctionsPerInstance
                                         : (concurrent_instance_count % kMaximumFunctionsPerInstance);
      }
      const std::string hostname = "skyrise-iperf3-server_" + RandomString(8);

      const std::string user_data_script =
          GenerateUserDataScript(kIperfServerDefaultPort, iperf_server_process_count, repetition_count);

      // Base64 encoding of user-data is required.
      Aws::Utils::ByteBuffer user_data_buffer(user_data_script.size());
      for (size_t j = 0; j < user_data_script.size(); ++j) {
        user_data_buffer[j] = user_data_script[j];
      }
      const auto encoded_user_data = Aws::Utils::Base64::Base64().Encode(user_data_buffer);

      auto instance_request =
          Aws::EC2::Model::RunInstancesRequest()
              .WithKeyName("skyrise-ci")
              .WithInstanceType(ec2_instance_type)
              .WithImageId(kAmazonLinux2023ImageIdArm)
              .WithTagSpecifications({Aws::EC2::Model::TagSpecification()
                                          .WithResourceType(Aws::EC2::Model::ResourceType::instance)
                                          .WithTags({Aws::EC2::Model::Tag().WithKey("Name").WithValue(hostname)})})
              .WithMinCount(1)
              .WithMaxCount(1)
              .WithUserData(encoded_user_data)
              .WithInstanceInitiatedShutdownBehavior(Aws::EC2::Model::ShutdownBehavior::terminate)
              .WithPlacement(Aws::EC2::Model::Placement().WithAvailabilityZone(kDefaultAvailabilityZone));

      if (enable_vpc_) {
        Aws::EC2::Model::InstanceNetworkInterfaceSpecification network_interface;
        network_interface.SetSubnetId(kDefaultSubnetId);
        network_interface.SetDeviceIndex(0);
        Aws::Vector<Aws::String> security_groups;
        security_groups.push_back(kIperfInboundSecurityGroupId);
        network_interface.SetGroups(security_groups);
        instance_request.AddNetworkInterfaces(network_interface);
      } else {
        instance_request.SetSecurityGroupIds(Aws::Vector<Aws::String>{kIperfInboundSecurityGroupId});
      }

      // Start EC2 instance.
      const auto result = ec2_client_->RunInstances(instance_request);
      Assert(result.IsSuccess(), result.GetError().GetMessage());
      Assert(result.GetResult().GetInstances().size() == 1,
             "Started more than 1 instance. Please terminate them manually to avoid costs.");
      const std::string ec2_instance_id = result.GetResult().GetInstances().at(0).GetInstanceId();

      Aws::EC2::Model::SummaryStatus status = Aws::EC2::Model::SummaryStatus::NOT_SET;
      while (status != Aws::EC2::Model::SummaryStatus::ok) {
        // TODO(tobodner): Reduce EC2 costs and check whether a status other than SummaryStatus::ok is sufficient.
        // It takes up to 5 min until the c6gn_16xlarge instance is initialized.
        std::this_thread::sleep_for(std::chrono::seconds(kInstanceStatusPollIntervalSeconds));
        Aws::EC2::Model::DescribeInstanceStatusRequest request;
        request.WithInstanceIds({ec2_instance_id});
        status = ec2_client_->DescribeInstanceStatus(request)
                     .GetResult()
                     .GetInstanceStatuses()
                     .at(0)
                     .GetInstanceStatus()
                     .GetStatus();
      }

      // Collect EC2 public IP, which is not available before initialization has completed.
      const auto private_address = result.GetResult().GetInstances().at(0).GetPrivateIpAddress();
      Aws::EC2::Model::DescribeInstancesRequest instances_request;
      instances_request.WithInstanceIds({ec2_instance_id});
      const auto describe_instances = ec2_client_->DescribeInstances(instances_request);
      for (const auto& reservation : describe_instances.GetResult().GetReservations()) {
        for (const auto& instance : reservation.GetInstances()) {
          if (instance.GetPrivateIpAddress() == private_address) {
            std::lock_guard<std::mutex> lock_guard(mutex_);
            ec2_instance_map_.insert({instance.GetPublicIpAddress(),
                                      {{ec2_instance_id, iperf_server_process_count}, instance.GetPrivateIpAddress()}});
          }
        }
      }

      return true;
    }));
  }

  for (auto& future : ec2_startup_futures) {
    const auto& result = future.get();
    Assert(result, "Error while starting EC2 cluster.");
  }
}

void LambdaNetworkBenchmark::Teardown() {
  std::vector<std::chrono::milliseconds> runtimes;
  std::vector<std::future<bool>> ec2_termination_futures;
  ec2_termination_futures.reserve(ec2_instance_map_.size());

  for (const auto& [public_ip, id_and_iperf_server_count] : ec2_instance_map_) {
    const std::string instance_id = id_and_iperf_server_count.first.first;
    const Aws::String ec2_public_ip = public_ip;
    ec2_termination_futures.emplace_back(std::async([this, instance_id, ec2_public_ip, &runtimes]() {
      Aws::EC2::Model::DescribeInstancesRequest request;
      request.WithInstanceIds({instance_id});
      const auto launch_time = ec2_client_->DescribeInstances(request)
                                   .GetResult()
                                   .GetReservations()
                                   .at(0)
                                   .GetInstances()
                                   .at(0)
                                   .GetLaunchTime();

      std::lock_guard<std::mutex> lock_guard(mutex_);
      runtimes.push_back(Aws::Utils::DateTime::Diff(Aws::Utils::DateTime::Now(), launch_time));

      const auto terminate_instances_outcome = ec2_client_->TerminateInstances(
          Aws::EC2::Model::TerminateInstancesRequest().WithInstanceIds(Aws::Vector<Aws::String>{instance_id}));
      Assert(terminate_instances_outcome.IsSuccess(), terminate_instances_outcome.GetError().GetMessage());

      ec2_instance_map_.erase(ec2_instance_map_.find(ec2_public_ip));
      return true;
    }));

    for (const auto& sqs_queue_url : sqs_queue_urls_) {
      sqs_client_->DeleteQueue(Aws::SQS::Model::DeleteQueueRequest().WithQueueUrl(sqs_queue_url));
    }
    sqs_queue_urls_.clear();
  }

  for (auto& future : ec2_termination_futures) {
    const auto& result = future.get();
    Assert(result, "Error while terminating EC2 cluster.");
  }

  Assert(ec2_instance_map_.empty(), "EC2 instance map must be empty after the teardown.");
  ec2_runtimes_.push_back(runtimes);
}

std::string LambdaNetworkBenchmark::GenerateUserDataScript(const size_t start_port, const size_t server_count,
                                                           const size_t repetition_count) {
  const std::string default_start_port = std::to_string(start_port);
  const std::string default_end_port = std::to_string(start_port + 1);
  Aws::String user_data_script = "#!/bin/bash\ndnf install -y iperf3 && for i in {" + default_start_port + ".." +
                                 default_end_port +
                                 "}; do iperf3 --server -p $i & done && echo \"shutdown -h\" | at now + " +
                                 std::to_string(kEc2TerminationGuardMinutes * repetition_count) + " minutes";
  return std::regex_replace(user_data_script, std::regex(default_end_port),
                            std::to_string(start_port + server_count - 1));
}

std::vector<Aws::EC2::Model::InstanceType> LambdaNetworkBenchmark::ResolveEc2InstanceTypes(
    const std::vector<size_t>& function_instance_mb_sizes, const std::vector<size_t>& concurrent_invocation_counts) {
  std::vector<Aws::EC2::Model::InstanceType> ec2_instance_types;
  ec2_instance_types.reserve(concurrent_invocation_counts.size() * function_instance_mb_sizes.size());
  ec2_cluster_size_.reserve(ec2_instance_types.size());

  for (size_t i = 0; i < function_instance_mb_sizes.size(); ++i) {
    for (const auto& concurrent_invocation_count : concurrent_invocation_counts) {
      if (concurrent_invocation_count > kMaximumFunctionsPerInstance) {
        // We require > 1 EC2 instances.
        ec2_cluster_size_.push_back(std::ceil(static_cast<double>(concurrent_invocation_count) /
                                              static_cast<double>(kMaximumFunctionsPerInstance)));
        ec2_instance_types.push_back(Aws::EC2::Model::InstanceType::c7gn_16xlarge);
      } else {
        // A single EC2 instance is sufficient for the benchmark configuration.
        // We ignore instances with network bursting mechanisms since their bandwidth is not guaranteed.
        ec2_cluster_size_.push_back(1);
        switch (concurrent_invocation_count) {
          case 1:
          case 2:
            ec2_instance_types.push_back(Aws::EC2::Model::InstanceType::c7gn_4xlarge);
            break;
          case 3:
          case 4:
            ec2_instance_types.push_back(Aws::EC2::Model::InstanceType::c7gn_8xlarge);
            break;
          case 5:
          case 6:
            ec2_instance_types.push_back(Aws::EC2::Model::InstanceType::c7gn_12xlarge);
            break;
          case 7:
          case 8:
            ec2_instance_types.push_back(Aws::EC2::Model::InstanceType::c7gn_16xlarge);
            break;
          default:
            Fail("A single EC2 instance can serve a maximum of 8 concurrent Lambda functions.");
        }
      }
    }
  }
  return ec2_instance_types;
}

std::string LambdaNetworkBenchmark::SetupSqsQueue() const {
  const std::string queue_name = kName + "-" + RandomString(10);
  Aws::SQS::Model::CreateQueueRequest request;
  request.WithQueueName(queue_name);
  const auto outcome = sqs_client_->CreateQueue(request);
  Assert(outcome.IsSuccess(), outcome.GetError().GetMessage());
  return outcome.GetResult().GetQueueUrl();
}

}  // namespace skyrise
