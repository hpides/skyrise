#include "benchmark_runner.hpp"

#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <mutex>
#include <regex>
#include <string>
#include <utility>

#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/iam/model/GetRoleRequest.h>
#include <aws/lambda/model/CreateFunctionRequest.h>
#include <aws/lambda/model/DeleteFunctionRequest.h>
#include <aws/lambda/model/DestinationConfig.h>
#include <aws/lambda/model/OnFailure.h>
#include <aws/lambda/model/OnSuccess.h>
#include <aws/lambda/model/PublishVersionRequest.h>
#include <aws/lambda/model/PutFunctionEventInvokeConfigRequest.h>
#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/sqs/model/CreateQueueResult.h>
#include <aws/sqs/model/DeleteMessageRequest.h>
#include <aws/sqs/model/DeleteQueueRequest.h>
#include <aws/sqs/model/GetQueueAttributesRequest.h>
#include <aws/sqs/model/QueueAttributeName.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>

#include "limits.hpp"
#include "utils/assert.hpp"

namespace skyrise {

// TODO(anyone): Add commit hash to logging tag
const std::string kTag = "SKYRISE/BENCHMARK/BENCHMARK_RUNNER";

BenchmarkRunner::BenchmarkRunner(std::shared_ptr<Client> client) : client_(std::move(client)) {
  const auto get_role_outcome =
      client_->GetIAMClient().GetRole(Aws::IAM::Model::GetRoleRequest().WithRoleName(kFunctionRoleName));

  Assert(get_role_outcome.IsSuccess(), get_role_outcome.GetError().GetMessage());

  function_role_arn_ = get_role_outcome.GetResult().GetRole().GetArn();
}

std::shared_ptr<BenchmarkResult> BenchmarkRunner::RunConfig(const BenchmarkConfig& config) {
  try {
    SetConfig(config);

    Setup();

    InvokeFunctions();

  } catch (const std::exception& e) {
    AWS_LOGSTREAM_ERROR(kTag.c_str(), e.what());
    Teardown();
    return nullptr;
  }

  Teardown();

  return std::move(benchmark_result_);
}

void BenchmarkRunner::SetConfig(const BenchmarkConfig& config) {
  Assert(config_history_.emplace(config.benchmark_id_).second,
         "BenchmarkConfig " + config.benchmark_id_ + " has already been run.");

  config_ = std::make_shared<BenchmarkConfig>(config);
  invoke_requests_.clear();
  invoke_requests_.shrink_to_fit();
}

void BenchmarkRunner::Setup() {
  AWS_LOGSTREAM_INFO(kTag.c_str(), "Creating functions...");

  // TODO(d-justen): Employ custom Lambda client if parallel function upload gets us in trouble
  const auto& lambda_client = client_->GetLambdaClient();

  std::vector<Aws::Lambda::Model::CreateFunctionOutcomeCallable> outcome_callables;

  for (const auto& function_config : config_->function_configs_) {
    auto create_function_request =
        Aws::Lambda::Model::CreateFunctionRequest()
            .WithFunctionName(function_config.function_name)
            .WithRuntime(Aws::Lambda::Model::Runtime::provided_al2)
            .WithRole(function_role_arn_)
            .WithHandler("HandlerFunction")
            .WithTimeout(kLambdaFunctionTimeoutSeconds)
            .WithMemorySize(function_config.memory_size)
            .WithCode(SetFunctionCode(function_config.function_path, function_config.function_name,
                                      function_config.is_local));

    if (config_->enable_tracing_) {
      create_function_request.SetTracingConfig(
          Aws::Lambda::Model::TracingConfig().WithMode(Aws::Lambda::Model::TracingMode::Active));
    }

    outcome_callables.emplace_back(lambda_client.CreateFunctionCallable(create_function_request));
  }

  for (auto& outcome_callable : outcome_callables) {
    const auto& outcome = outcome_callable.get();
    Assert(outcome.IsSuccess(), outcome.GetError().GetMessage());

    const auto publish_version_outcome = lambda_client.PublishVersion(
        Aws::Lambda::Model::PublishVersionRequest().WithFunctionName(outcome.GetResult().GetFunctionName()));
    Assert(publish_version_outcome.IsSuccess(), publish_version_outcome.GetError().GetMessage());
  }

  AWS_LOGSTREAM_INFO(kTag.c_str(), "Functions created.");

  if (config_->use_event_queue_ == UseEventQueue::kYes) {
    SetupEventQueue();
  }

  CreateInvokeRequests();
}

void BenchmarkRunner::SetupEventQueue() {
  const Aws::String queue_name = config_->benchmark_id_ + "-" + config_->benchmark_timestamp_;

  AWS_LOGSTREAM_INFO(kTag.c_str(), "Creating queue " << queue_name << "...");

  const auto& sqs_client = client_->GetSQSClient();
  const auto& lambda_client = client_->GetLambdaClient();

  const auto create_queue_outcome =
      sqs_client.CreateQueue(Aws::SQS::Model::CreateQueueRequest().WithQueueName(queue_name));

  Assert(create_queue_outcome.IsSuccess(), create_queue_outcome.GetError().GetMessage());

  sqs_queue_url_ = std::make_shared<Aws::String>(create_queue_outcome.GetResult().GetQueueUrl());
  AWS_LOGSTREAM_INFO(kTag.c_str(), "Queue " << queue_name << " created.");

  const auto queue_attributes_outcome =
      sqs_client.GetQueueAttributes(Aws::SQS::Model::GetQueueAttributesRequest()
                                        .WithQueueUrl(*sqs_queue_url_)
                                        .WithAttributeNames(std::vector<Aws::SQS::Model::QueueAttributeName>(
                                            1, Aws::SQS::Model::QueueAttributeName::QueueArn)));

  Assert(queue_attributes_outcome.IsSuccess(), queue_attributes_outcome.GetError().GetMessage());

  const auto queue_arn =
      queue_attributes_outcome.GetResult().GetAttributes().at(Aws::SQS::Model::QueueAttributeName::QueueArn);

  for (const auto& function_config : config_->function_configs_) {
    lambda_client.PutFunctionEventInvokeConfig(
        Aws::Lambda::Model::PutFunctionEventInvokeConfigRequest()
            .WithFunctionName(function_config.function_name)
            .WithQualifier("1")
            .WithDestinationConfig(Aws::Lambda::Model::DestinationConfig()
                                       .WithOnSuccess(Aws::Lambda::Model::OnSuccess().WithDestination(queue_arn))
                                       .WithOnFailure(Aws::Lambda::Model::OnFailure().WithDestination(queue_arn))));
  }
}

void BenchmarkRunner::Teardown() {
  if (sqs_queue_url_) {
    AWS_LOGSTREAM_INFO(kTag.c_str(), "Deleting queue " << *sqs_queue_url_ << "...");

    const auto outcome =
        client_->GetSQSClient().DeleteQueue(Aws::SQS::Model::DeleteQueueRequest().WithQueueUrl(*sqs_queue_url_));
    if (outcome.IsSuccess()) {
      AWS_LOGSTREAM_INFO(kTag.c_str(), "Queue " << *sqs_queue_url_ << " deleted.");
    } else {
      AWS_LOGSTREAM_ERROR(kTag.c_str(), outcome.GetError().GetMessage());
    }

    sqs_queue_url_.reset();
  }
}

void BenchmarkRunner::InvokeFunctions() {
  AWS_LOGSTREAM_INFO(kTag.c_str(), "Invoking functions concurrently...");

  const auto& lambda_client = client_->GetLambdaClient();

  benchmark_result_ =
      std::make_shared<BenchmarkResult>(config_->repetition_count_, config_->concurrent_invocation_count_);

  // BENCHMARK STARTS

  for (size_t i = 0; i < config_->repetition_count_; i++) {
    if (config_->warm_up_ != WarmUp::kNone) {
      WarmUpFunctions(i);
    }

    AWS_LOGSTREAM_INFO(kTag.c_str(), "Repetition " << i << " started.");

    for (const auto& [invocation_id, invoke_request] : invoke_requests_[i]) {
      benchmark_result_->RegisterInvocation(i, invocation_id);

      lambda_client.InvokeAsync(
          invoke_request,
          [&](const Aws::Lambda::LambdaClient* /*unused*/, const Aws::Lambda::Model::InvokeRequest& /*unused*/,
              Aws::Lambda::Model::InvokeOutcome outcome,
              const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) {
            const auto context_function_invocation =
                std::dynamic_pointer_cast<const ContextFunctionInvocation>(context);

            const bool is_success = outcome.IsSuccess();

            const auto invoke_result =
                is_success ? std::make_shared<Aws::Lambda::Model::InvokeResult>(outcome.GetResultWithOwnership())
                           : nullptr;
            benchmark_result_->FinishInvocation(context_function_invocation->GetRepetition(),
                                                context_function_invocation->GetUUID(), invoke_result, is_success);
          },
          std::make_shared<const ContextFunctionInvocation>(i, invocation_id));
    }

    while (!benchmark_result_->HasRepetitionFinished(i)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    if (config_->use_event_queue_ == UseEventQueue::kYes) {
      const auto sqs_messages = CollectSqsMessages(invoke_requests_[i].size());

      for (const auto& sqs_message : *sqs_messages) {
        benchmark_result_->UpdateSQSMessageBody(i, sqs_message.first, sqs_message.second);
      }
    }

    config_->after_repetition_callbacks_[i]();

    AWS_LOGSTREAM_INFO(kTag.c_str(), "Repetition " << i << " finished in "
                                                   << benchmark_result_->GetRepetitionDuration(i).count()
                                                   << " seconds.");
    if (config_->use_one_function_per_repetition_ == UseOneFunctionPerRepetition::kYes ||
        i == config_->repetition_count_ - 1) {
      const size_t function_index =
          config_->use_one_function_per_repetition_ == UseOneFunctionPerRepetition::kYes ? i : 0;
      AWS_LOGSTREAM_INFO(kTag.c_str(),
                         "Delete function " << config_->function_configs_[function_index].function_name << "...");

      lambda_client.DeleteFunction(Aws::Lambda::Model::DeleteFunctionRequest().WithFunctionName(
          config_->function_configs_[function_index].function_name));

      AWS_LOGSTREAM_INFO(kTag.c_str(), "Function deleted.");
    }
  }

  // BENCHMARK ENDS
  AWS_LOGSTREAM_INFO(kTag.c_str(),
                     "Benchmark finished in " << benchmark_result_->GetBenchmarkDuration().count() << " seconds.");
}

void BenchmarkRunner::WarmUpFunctions(const size_t repetition) {
  AWS_LOGSTREAM_INFO(kTag.c_str(), "Warming up functions for repetition " << repetition << "...");

  const size_t function_index =
      config_->use_one_function_per_repetition_ == UseOneFunctionPerRepetition::kYes ? repetition : 0;

  long double function_warm_up_cost = config_->warm_up_strategy_->WarmUpFunctions(
      client_, config_->function_configs_[function_index], config_->concurrent_invocation_count_);

  benchmark_result_->SetFunctionWarmUpCost(repetition, function_warm_up_cost);

  AWS_LOGSTREAM_INFO(kTag.c_str(), "Functions warmed up.");
}

Aws::Lambda::Model::InvokeRequest BenchmarkRunner::CreateInvokeRequest(const Aws::String& function_name,
                                                                       const Aws::String& invocation_id,
                                                                       const std::shared_ptr<Aws::IOStream>& payload) {
  const auto invocation_type = config_->use_event_queue_ == UseEventQueue::kYes
                                   ? Aws::Lambda::Model::InvocationType::Event
                                   : Aws::Lambda::Model::InvocationType::RequestResponse;

  const auto json_value =
      Aws::Utils::Json::JsonValue(StreamToString(payload.get())).WithString("invocation_id", invocation_id);
  const auto body = std::make_shared<Aws::StringStream>(json_value.View().WriteCompact());

  auto invoke_request = Aws::Lambda::Model::InvokeRequest()
                            .WithFunctionName(function_name)
                            .WithInvocationType(invocation_type)
                            .WithQualifier("1")
                            .WithLogType(Aws::Lambda::Model::LogType::Tail);
  invoke_request.SetBody(body);
  invoke_request.SetContentType("application/json");

  return invoke_request;
}

void BenchmarkRunner::CreateInvokeRequests() {
  AWS_LOGSTREAM_INFO(kTag.c_str(), "Creating invoke requests...");
  invoke_requests_.reserve(config_->repetition_count_);

  for (size_t i = 0; i < config_->repetition_count_; i++) {
    std::vector<std::pair<Aws::String, Aws::Lambda::Model::InvokeRequest>> requests;
    requests.reserve(config_->concurrent_invocation_count_);

    for (const auto& config : config_->repetition_configs_[i]) {
      const Aws::String invocation_id = std::to_string(i) + "-" + config.invocation_id;
      requests.emplace_back(invocation_id, CreateInvokeRequest(config.function_name, invocation_id, config.payload));
    }

    invoke_requests_.emplace_back(requests);
  }

  AWS_LOGSTREAM_INFO(kTag.c_str(), "Invoke requests created.");
}

std::shared_ptr<std::unordered_map<Aws::String, Aws::String>> BenchmarkRunner::CollectSqsMessages(
    const size_t invocation_count) {
  auto sqs_messages = std::make_shared<std::unordered_map<Aws::String, Aws::String>>();
  sqs_messages->reserve(invocation_count);

  const auto& sqs_client = client_->GetSQSClient();
  size_t receive_message_requests = 0;

  while (sqs_messages->size() < invocation_count && receive_message_requests < kLambdaFunctionTimeoutSeconds) {
    receive_message_requests++;
    const auto receive_message_outcome = sqs_client.ReceiveMessage(Aws::SQS::Model::ReceiveMessageRequest()
                                                                       .WithQueueUrl(*sqs_queue_url_)
                                                                       .WithWaitTimeSeconds(1)
                                                                       .WithMaxNumberOfMessages(10));
    const auto messages = receive_message_outcome.GetResult().GetMessages();

    for (const auto& message : messages) {
      const auto json_value = Aws::Utils::Json::JsonValue(message.GetBody());
      const auto invocation_id = json_value.View().GetObject("requestPayload").GetString("invocation_id");

      sqs_messages->emplace(invocation_id, message.GetBody());

      const auto delete_message_outcome = sqs_client.DeleteMessage(Aws::SQS::Model::DeleteMessageRequest()
                                                                       .WithQueueUrl(*sqs_queue_url_)
                                                                       .WithReceiptHandle(message.GetReceiptHandle()));

      Assert(delete_message_outcome.IsSuccess(), delete_message_outcome.GetError().GetMessage());
    }
  }
  return sqs_messages;
}

Aws::Utils::CryptoBuffer BenchmarkRunner::OpenFunctionZip(const Aws::String& function_path) {
  std::ifstream infile(function_path, std::ios::in | std::ios::binary);
  Assert(infile, function_path + " could not be opened.");

  const std::string file_buffer = StreamToString(&infile);

  return Aws::Utils::ByteBuffer(reinterpret_cast<const unsigned char*>(file_buffer.c_str()), file_buffer.length());
}

Aws::Lambda::Model::FunctionCode BenchmarkRunner::SetFunctionCode(const Aws::String& function_path,
                                                                  const Aws::String& function_name,
                                                                  const bool is_local) {
  Aws::Lambda::Model::FunctionCode code;

  if (is_local) {
    const std::lock_guard<std::mutex> lock(package_files_mutex_);

    if (package_files_.find(function_path) == package_files_.cend()) {
      package_files_[function_path] = OpenFunctionZip(function_path);
    }

    code.WithZipFile(package_files_[function_path]);
  } else {
    // Extract function name after S3_
    std::regex function_name_regex("S3_([^-]*)");
    std::smatch matches;

    const auto function_name_found = std::regex_search(function_name, matches, function_name_regex);
    Assert(function_name_found, "S3 key could not be extracted from function name " + function_name + ".");

    code.WithS3Bucket(function_path).WithS3Key(matches[1]);
  }

  return code;
}

}  // namespace skyrise
