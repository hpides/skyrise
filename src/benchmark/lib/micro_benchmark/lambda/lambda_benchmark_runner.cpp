#include "lambda_benchmark_runner.hpp"

#include <cmath>
#include <functional>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/lambda/model/DeleteFunctionRequest.h>
#include <aws/lambda/model/DestinationConfig.h>
#include <aws/lambda/model/OnFailure.h>
#include <aws/lambda/model/OnSuccess.h>
#include <aws/lambda/model/PutFunctionConcurrencyRequest.h>
#include <aws/lambda/model/PutFunctionEventInvokeConfigRequest.h>
#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/sqs/model/DeleteMessageRequest.h>
#include <aws/sqs/model/DeleteQueueRequest.h>
#include <aws/sqs/model/GetQueueAttributesRequest.h>
#include <aws/sqs/model/QueueAttributeName.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>

#include "configuration.hpp"
#include "function/function_utils.hpp"
#include "utils/assert.hpp"
#include "utils/region.hpp"

namespace skyrise {

LambdaBenchmarkRunner::LambdaBenchmarkRunner(std::shared_ptr<const Aws::IAM::IAMClient> iam_client,
                                             std::shared_ptr<const Aws::Lambda::LambdaClient> lambda_client,
                                             std::shared_ptr<const Aws::SQS::SQSClient> sqs_client,
                                             std::shared_ptr<const CostCalculator> cost_calculator,
                                             const Aws::String& client_region, const bool metering,
                                             const bool introspection)
    : AbstractBenchmarkRunner(metering, introspection),
      iam_client_(std::move(iam_client)),
      lambda_client_(std::move(lambda_client)),
      sqs_client_(std::move(sqs_client)),
      cost_calculator_(std::move(cost_calculator)),
      client_region_(client_region) {}

std::shared_ptr<LambdaBenchmarkResult> LambdaBenchmarkRunner::RunLambdaConfig(
    const std::shared_ptr<LambdaBenchmarkConfig>& config) {
  return std::dynamic_pointer_cast<LambdaBenchmarkResult>(RunConfig(config));
}

void LambdaBenchmarkRunner::Setup() {
  typed_config_ = std::dynamic_pointer_cast<LambdaBenchmarkConfig>(config_);
  Assert(typed_config_, "LambdaBenchmarkRunner can only consume LambdaBenchmarkConfigs.");

  AWS_LOGSTREAM_INFO(kBenchmarkTag.c_str(), "Creating functions...");

  // TODO(tobodner): Employ custom Lambda client if parallel function upload gets us in trouble
  std::vector<FunctionDeployable> function_deployables;

  for (const auto& function_config : typed_config_->function_configs_) {
    Aws::Lambda::Model::FunctionCode function_code;
    if (function_config.is_local_code) {
      const std::lock_guard<std::mutex> lock(package_files_mutex_);
      if (package_files_.find(function_config.path) == package_files_.cend()) {
        package_files_[function_config.path] = OpenFunctionZipFile(function_config.path);
      }
      function_code.WithZipFile(package_files_[function_config.path]);
    } else {
      function_code = GetRemoteFunctionCode(function_config.path, function_config.name);
    }

    function_deployables.emplace_back(function_config.name, function_code.Jsonize(), function_config.memory_size,
                                      function_config.is_vpc_connected, function_config.description,
                                      function_config.efs_arn);
  }

  UploadFunctions(iam_client_, lambda_client_, function_deployables, typed_config_->enable_tracing_);

  AWS_LOGSTREAM_INFO(kBenchmarkTag.c_str(), "Functions created and active.");

  if (typed_config_->use_event_queue_ == EventQueue::kYes) {
    SetupEventQueue();
  }

  CreateInvocationRequests();
}

void LambdaBenchmarkRunner::SetupEventQueue() {
  const Aws::String queue_name = typed_config_->benchmark_id_ + "-" + typed_config_->benchmark_timestamp_;

  AWS_LOGSTREAM_INFO(kBenchmarkTag.c_str(), "Creating queue " << queue_name << "...");

  const auto create_queue_outcome =
      sqs_client_->CreateQueue(Aws::SQS::Model::CreateQueueRequest().WithQueueName(queue_name));

  Assert(create_queue_outcome.IsSuccess(), create_queue_outcome.GetError().GetMessage());

  sqs_queue_url_ = std::make_shared<Aws::String>(create_queue_outcome.GetResult().GetQueueUrl());
  AWS_LOGSTREAM_INFO(kBenchmarkTag.c_str(), "Queue " << queue_name << " created.");

  const auto queue_attributes_outcome =
      sqs_client_->GetQueueAttributes(Aws::SQS::Model::GetQueueAttributesRequest()
                                          .WithQueueUrl(*sqs_queue_url_)
                                          .WithAttributeNames(std::vector<Aws::SQS::Model::QueueAttributeName>(
                                              1, Aws::SQS::Model::QueueAttributeName::QueueArn)));

  Assert(queue_attributes_outcome.IsSuccess(), queue_attributes_outcome.GetError().GetMessage());

  const auto queue_arn =
      queue_attributes_outcome.GetResult().GetAttributes().at(Aws::SQS::Model::QueueAttributeName::QueueArn);

  for (const auto& function_config : typed_config_->function_configs_) {
    lambda_client_->PutFunctionEventInvokeConfig(
        Aws::Lambda::Model::PutFunctionEventInvokeConfigRequest()
            .WithFunctionName(function_config.name)
            .WithQualifier("1")
            .WithDestinationConfig(Aws::Lambda::Model::DestinationConfig()
                                       .WithOnSuccess(Aws::Lambda::Model::OnSuccess().WithDestination(queue_arn))
                                       .WithOnFailure(Aws::Lambda::Model::OnFailure().WithDestination(queue_arn))));
  }
}

void LambdaBenchmarkRunner::Teardown() {
  if (sqs_queue_url_) {
    AWS_LOGSTREAM_INFO(kBenchmarkTag.c_str(), "Deleting queue " << *sqs_queue_url_ << "...");

    const auto outcome = sqs_client_->DeleteQueue(Aws::SQS::Model::DeleteQueueRequest().WithQueueUrl(*sqs_queue_url_));
    if (outcome.IsSuccess()) {
      AWS_LOGSTREAM_INFO(kBenchmarkTag.c_str(), "Queue " << *sqs_queue_url_ << " deleted.");
    } else {
      AWS_LOGSTREAM_ERROR(kBenchmarkTag.c_str(), outcome.GetError().GetMessage());
    }

    sqs_queue_url_.reset();
  }

  invocation_requests_.clear();
  invocation_requests_.shrink_to_fit();
}

std::shared_ptr<AbstractBenchmarkResult> LambdaBenchmarkRunner::OnRunConfig() {
  AWS_LOGSTREAM_INFO(kBenchmarkTag.c_str(), "Invoking functions concurrently...");

  benchmark_result_ = std::make_shared<LambdaBenchmarkResult>(typed_config_->concurrent_instance_count_,
                                                              typed_config_->repetition_count_, cost_calculator_);

  // BENCHMARK STARTS
  for (size_t i = 0; i < typed_config_->repetition_count_; ++i) {
    if (typed_config_->distinct_function_per_repetition_ == DistinctFunctionPerRepetition::kYes || i == 0) {
      const size_t function_concurrency = [&]() {
        if (typed_config_->warmup_strategy_ &&
            typed_config_->warmup_strategy_->Name() == "ConfigurableWarmUpStrategy") {
          return static_cast<size_t>(invocation_requests_[i].size() *
                                     ConfigurableWarmupStrategy::kDefaultWarmupProvisioningFactor);
        } else {
          return invocation_requests_[i].size();
        }
      }();

      const auto put_function_concurrency_outcome =
          lambda_client_->PutFunctionConcurrency(Aws::Lambda::Model::PutFunctionConcurrencyRequest()
                                                     .WithFunctionName(typed_config_->function_configs_[i].name)
                                                     .WithReservedConcurrentExecutions(function_concurrency));
      Assert(put_function_concurrency_outcome.IsSuccess(), put_function_concurrency_outcome.GetError().GetMessage());
    }

    if (typed_config_->warmup_type_ != Warmup::kNo) {
      WarmupFunctions(i);
    }

    AWS_LOGSTREAM_INFO(kBenchmarkTag.c_str(), "Repetition " << i << " started.");

    for (size_t j = 0; j < invocation_requests_[i].size(); ++j) {
      benchmark_result_->RegisterInvocation(i, j, invocation_requests_[i][j].first);
      lambda_client_->InvokeAsync(
          invocation_requests_[i][j].second,
          [&](const Aws::Lambda::LambdaClient* /*unused*/, const Aws::Lambda::Model::InvokeRequest& /*unused*/,
              Aws::Lambda::Model::InvokeOutcome outcome,
              const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) {
            const auto context_function_invocation =
                std::dynamic_pointer_cast<const ContextFunctionInvocation>(context);
            benchmark_result_->CompleteInvocation(context_function_invocation->GetRepetition(),
                                                  context_function_invocation->GetInvocation(), &outcome);
          },
          std::make_shared<const ContextFunctionInvocation>(i, j, invocation_requests_[i][j].first));
    }
    while (!benchmark_result_->HasRepetitionFinished(i)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (typed_config_->use_event_queue_ == EventQueue::kYes) {
      CollectSqsMessages(invocation_requests_[i].size());
    }
    typed_config_->after_repetition_callbacks_[i]();

    AWS_LOGSTREAM_INFO(kBenchmarkTag.c_str(),
                       "Repetition " << i << " finished in "
                                     << benchmark_result_->GetRepetitionResults()[i].GetDurationMs() << " ms.");
    if (typed_config_->distinct_function_per_repetition_ == DistinctFunctionPerRepetition::kYes ||
        i == typed_config_->repetition_count_ - 1) {
      const size_t function_index =
          typed_config_->distinct_function_per_repetition_ == DistinctFunctionPerRepetition::kYes ? i : 0;
      AWS_LOGSTREAM_INFO(kBenchmarkTag.c_str(),
                         "Delete function " << typed_config_->function_configs_[function_index].name << "...");

      lambda_client_->DeleteFunction(Aws::Lambda::Model::DeleteFunctionRequest().WithFunctionName(
          typed_config_->function_configs_[function_index].name));

      AWS_LOGSTREAM_INFO(kBenchmarkTag.c_str(), "Function deleted.");
    }
  }

  // BENCHMARK ENDS
  benchmark_result_->SetEndTime();
  AWS_LOGSTREAM_INFO(kBenchmarkTag.c_str(), "Benchmark finished in " << benchmark_result_->GetDurationMs() << " ms.");

  return benchmark_result_;
}

void LambdaBenchmarkRunner::WarmupFunctions(const size_t repetition) {
  AWS_LOGSTREAM_INFO(kBenchmarkTag.c_str(), "Warming up functions for repetition " << repetition << "...");

  const size_t function_index =
      typed_config_->distinct_function_per_repetition_ == DistinctFunctionPerRepetition::kYes ? repetition : 0;

  const bool is_same_aws_region = GetAwsRegion() == client_region_;

  const long double repetition_warmup_cost_usd = typed_config_->warmup_strategy_->WarmupFunctions(
      lambda_client_, cost_calculator_, typed_config_->function_configs_[function_index],
      typed_config_->concurrent_instance_count_, is_same_aws_region);

  benchmark_result_->SetRepetitionWarmupCost(repetition, repetition_warmup_cost_usd);

  AWS_LOGSTREAM_INFO(kBenchmarkTag.c_str(), "Functions warmed up.");
}

void LambdaBenchmarkRunner::CreateInvocationRequests() {
  AWS_LOGSTREAM_INFO(kBenchmarkTag.c_str(), "Creating invocation requests...");
  invocation_requests_.reserve(typed_config_->repetition_count_);

  const auto invocation_type = typed_config_->use_event_queue_ == EventQueue::kYes
                                   ? Aws::Lambda::Model::InvocationType::Event
                                   : Aws::Lambda::Model::InvocationType::RequestResponse;

  const bool is_same_aws_region = GetAwsRegion() == client_region_;

  for (size_t i = 0; i < typed_config_->repetition_count_; ++i) {
    std::vector<std::pair<Aws::String, Aws::Lambda::Model::InvokeRequest>> requests;
    requests.reserve(typed_config_->concurrent_instance_count_);

    const auto& repetition_config = typed_config_->repetition_configs_[i];

    for (size_t j = 0; j < repetition_config.size(); ++j) {
      const auto& function_invocation_config = repetition_config[j];
      const Aws::String instance_id = function_invocation_config.instance_id;
      const size_t concurrency_duration_seconds =
          std::ceil(is_same_aws_region ? std::cbrt(typed_config_->concurrent_instance_count_)
                                       : std::sqrt(typed_config_->concurrent_instance_count_));

      const auto request_body =
          Aws::Utils::Json::JsonValue(StreamToString(function_invocation_config.request_body.get()))
              .WithString("instance_id", instance_id)
              .WithInteger("invocation", j)
              .WithInteger("repetition", i)
              .WithInteger("concurrency_duration_seconds", concurrency_duration_seconds)
              .WithBool(kRequestEnableIntrospection, IsIntrospectionEnabled())
              .WithBool(kRequestEnableMetering, IsMeteringEnabled());

      const auto annotated_request_body = std::make_shared<Aws::StringStream>(request_body.View().WriteCompact());

      auto invoke_request = Aws::Lambda::Model::InvokeRequest()
                                .WithFunctionName(function_invocation_config.name)
                                .WithInvocationType(invocation_type)
                                .WithQualifier("1")
                                .WithLogType(Aws::Lambda::Model::LogType::Tail);
      invoke_request.SetBody(annotated_request_body);
      invoke_request.SetContentType("application/json");

      requests.emplace_back(instance_id, invoke_request);
    }

    invocation_requests_.emplace_back(requests);
  }

  AWS_LOGSTREAM_INFO(kBenchmarkTag.c_str(), "Invocation requests created.");
}

void LambdaBenchmarkRunner::CollectSqsMessages(const size_t invocation_count) {
  size_t receive_message_requests = 0;
  size_t sqs_message_count = 0;

  while (sqs_message_count < invocation_count && receive_message_requests < kLambdaFunctionTimeoutSeconds) {
    receive_message_requests++;
    const auto receive_message_outcome = sqs_client_->ReceiveMessage(Aws::SQS::Model::ReceiveMessageRequest()
                                                                         .WithQueueUrl(*sqs_queue_url_)
                                                                         .WithWaitTimeSeconds(1)
                                                                         .WithMaxNumberOfMessages(10));
    const auto messages = receive_message_outcome.GetResult().GetMessages();

    for (const auto& message : messages) {
      sqs_message_count++;

      const auto json_value = Aws::Utils::Json::JsonValue(message.GetBody());
      const auto request_payload = json_value.View().GetObject("requestPayload");
      const size_t repetition = request_payload.GetInteger("repetition");
      const size_t invocation = request_payload.GetInteger("invocation");

      benchmark_result_->UpdateSQSMessageBody(repetition, invocation, message.GetBody());

      const auto delete_message_outcome =
          sqs_client_->DeleteMessage(Aws::SQS::Model::DeleteMessageRequest()
                                         .WithQueueUrl(*sqs_queue_url_)
                                         .WithReceiptHandle(message.GetReceiptHandle()));

      Assert(delete_message_outcome.IsSuccess(), delete_message_outcome.GetError().GetMessage());
    }
  }
}

}  // namespace skyrise
