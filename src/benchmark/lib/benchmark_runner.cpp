#include "benchmark_runner.hpp"

#include <fstream>
#include <functional>
#include <future>
#include <iostream>
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
#include <aws/lambda/model/PutFunctionEventInvokeConfigRequest.h>
#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/sqs/model/CreateQueueResult.h>
#include <aws/sqs/model/DeleteMessageRequest.h>
#include <aws/sqs/model/DeleteQueueRequest.h>
#include <aws/sqs/model/GetQueueAttributesRequest.h>
#include <aws/sqs/model/Message.h>
#include <aws/sqs/model/QueueAttributeName.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <aws/sqs/model/ReceiveMessageResult.h>

#include "limits.hpp"
#include "utils/assert.hpp"

namespace skyrise {

// TODO(anyone): Add commit hash to logging tag
const std::string kTag = "SKYRISE/BENCHMARK/BENCHMARK_RUNNER";

BenchmarkRunner::BenchmarkRunner(std::shared_ptr<Client> client) : client_(std::move(client)) {
  const auto get_role_outcome =
      client_->GetIAMClient().GetRole(Aws::IAM::Model::GetRoleRequest().WithRoleName(kFunctionRoleName));

  if (!get_role_outcome.IsSuccess()) {
    Fail(get_role_outcome.GetError().GetMessage());
  }

  function_role_arn_ = get_role_outcome.GetResult().GetRole().GetArn();
}

std::shared_ptr<BenchmarkResult> BenchmarkRunner::RunConfig(const BenchmarkConfig& config) {
  try {
    SetConfig(config);

    Setup();

    RunParallel();

  } catch (const std::exception& e) {
    AWS_LOGSTREAM_ERROR(kTag.c_str(), e.what());
    Teardown();
    return std::make_shared<BenchmarkResult>(0, 0);
  }

  Teardown();

  return std::move(benchmark_result_);
}

void BenchmarkRunner::SetConfig(const BenchmarkConfig& config) {
  if (config_history_.emplace(config.benchmark_id_).second) {
    config_ = std::make_shared<BenchmarkConfig>(config);
  } else {
    Fail("BenchmarkConfig " + config.benchmark_id_ + " has already been run.");
  }
}

void BenchmarkRunner::Setup() {
  AWS_LOGSTREAM_INFO(kTag.c_str(), "Creating functions...");

  size_t function_count = config_->function_configs_.size();
  size_t thread_count = function_count > setup_thread_count_ ? setup_thread_count_ : function_count;
  std::vector<std::future<std::vector<Aws::Lambda::Model::CreateFunctionOutcome>>> outcome_vec_futures;
  outcome_vec_futures.reserve(thread_count);

  Aws::Lambda::Model::TracingConfig tracing_config;

  if (config_->enable_tracing_) {
    tracing_config.WithMode(Aws::Lambda::Model::TracingMode::Active);
  }

  for (size_t i = 0; i < thread_count; i++) {
    outcome_vec_futures.emplace_back(
        std::async(&BenchmarkRunner::UploadFunctions, this, thread_count, i, tracing_config));
  }

  // We first wait for all threads to finish so that we do not tear down functions that are still uploading in case of
  // an error
  for (const auto& outcome_vec_future : outcome_vec_futures) {
    outcome_vec_future.wait();
  }

  for (auto& outcome_vec_future : outcome_vec_futures) {
    const auto& outcome_vec = outcome_vec_future.get();

    for (const auto& outcome : outcome_vec) {
      if (!outcome.IsSuccess()) {
        Fail(outcome.GetError().GetMessage());
      }
    }
  }

  AWS_LOGSTREAM_INFO(kTag.c_str(), "Functions created.");

  if (IsAsyncBenchmark()) {
    SetupAsync();
  }

  if (IsWarmStartBenchmark()) {
    CreateInvokeRequests(true);
  }

  CreateInvokeRequests(false);
}

void BenchmarkRunner::SetupAsync() {
  const Aws::String queue_name = config_->benchmark_id_ + "-" + config_->benchmark_timestamp_;

  AWS_LOGSTREAM_INFO(kTag.c_str(), "Creating queue " << queue_name << "...");

  const auto& sqs_client = client_->GetSQSClient();
  const auto& lambda_client = client_->GetLambdaClient();

  const auto create_queue_outcome =
      sqs_client.CreateQueue(Aws::SQS::Model::CreateQueueRequest().WithQueueName(queue_name));

  if (create_queue_outcome.IsSuccess()) {
    sqs_queue_url_ = std::make_shared<Aws::String>(create_queue_outcome.GetResult().GetQueueUrl());
    AWS_LOGSTREAM_INFO(kTag.c_str(), "Queue " << queue_name << " created.");
  } else {
    Fail(create_queue_outcome.GetError().GetMessage());
  }

  const auto queue_attributes_outcome =
      sqs_client.GetQueueAttributes(Aws::SQS::Model::GetQueueAttributesRequest()
                                        .WithQueueUrl(*sqs_queue_url_)
                                        .WithAttributeNames(std::vector<Aws::SQS::Model::QueueAttributeName>(
                                            1, Aws::SQS::Model::QueueAttributeName::QueueArn)));

  if (!queue_attributes_outcome.IsSuccess()) {
    Fail(queue_attributes_outcome.GetError().GetMessage());
  }

  const auto queue_arn =
      queue_attributes_outcome.GetResult().GetAttributes().at(Aws::SQS::Model::QueueAttributeName::QueueArn);

  for (const auto& function_config : config_->function_configs_) {
    lambda_client.PutFunctionEventInvokeConfig(
        Aws::Lambda::Model::PutFunctionEventInvokeConfigRequest()
            .WithFunctionName(function_config.function_name)
            .WithDestinationConfig(Aws::Lambda::Model::DestinationConfig()
                                       .WithOnSuccess(Aws::Lambda::Model::OnSuccess().WithDestination(queue_arn))
                                       .WithOnFailure(Aws::Lambda::Model::OnFailure().WithDestination(queue_arn))));
  }
}

void BenchmarkRunner::Teardown() {
  AWS_LOGSTREAM_INFO(kTag.c_str(), "Deleting functions...");

  std::vector<std::pair<Aws::String, std::future<Aws::Lambda::Model::DeleteFunctionOutcome>>> delete_function_outcomes;
  delete_function_outcomes.reserve(config_->function_configs_.size());

  const auto& lambda_client = client_->GetLambdaClient();

  for (const auto& function_config : config_->function_configs_) {
    AWS_LOGSTREAM_INFO(kTag.c_str(), "Deleting function " << function_config.function_name << "...")

    delete_function_outcomes.emplace_back(
        function_config.function_name, std::async([&]() {
          const auto delete_function_request =
              Aws::Lambda::Model::DeleteFunctionRequest().WithFunctionName(function_config.function_name);

          return lambda_client.DeleteFunction(delete_function_request);
        }));
  }

  for (auto& outcome_pair : delete_function_outcomes) {
    const auto outcome = outcome_pair.second.get();

    if (outcome.IsSuccess()) {
      AWS_LOGSTREAM_INFO(kTag.c_str(), "Function " << outcome_pair.first << " deleted.");
    } else {
      AWS_LOGSTREAM_ERROR(kTag.c_str(), outcome.GetError().GetMessage());
    }
  }

  AWS_LOGSTREAM_INFO(kTag.c_str(), "Functions deleted.");

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

void BenchmarkRunner::RunParallel() {
  AWS_LOGSTREAM_INFO(kTag.c_str(), "Invoking functions concurrently...");

  const auto& lambda_client = client_->GetLambdaClient();

  size_t invocations_finished = 0;
  std::mutex invocations_finished_mutex;

  const auto increment_invocations_finished = [&]() {
    std::lock_guard<std::mutex> lock(invocations_finished_mutex);
    invocations_finished++;
  };

  benchmark_result_ =
      std::make_shared<BenchmarkResult>(config_->repetition_count_, config_->concurrent_invocation_count_);

  // BENCHMARK STARTS

  for (size_t i = 0; i < config_->repetition_count_; i++) {
    if (IsWarmStartBenchmark()) {
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
            const auto invoke_result =
                std::make_shared<Aws::Lambda::Model::InvokeResult>(outcome.GetResultWithOwnership());
            benchmark_result_->FinishInvocation(context_function_invocation->GetRepetition(),
                                                context_function_invocation->GetUUID(), invoke_result,
                                                outcome.IsSuccess());
            increment_invocations_finished();
          },
          std::make_shared<const ContextFunctionInvocation>(i, invocation_id));
    }

    while (invocations_finished < config_->concurrent_invocation_count_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    invocations_finished = 0;

    if (IsAsyncBenchmark()) {
      const auto sqs_messages = CollectSqsMessages(invoke_requests_[i].size());

      for (const auto& sqs_message : *sqs_messages) {
        benchmark_result_->UpdateSQSMessageBody(i, sqs_message.first, sqs_message.second);
      }
    }

    AWS_LOGSTREAM_INFO(kTag.c_str(), "Repetition " << i << " finished in "
                                                   << benchmark_result_->GetRepetitionDuration(i).count()
                                                   << " seconds.");
  }

  // BENCHMARK ENDS
  AWS_LOGSTREAM_INFO(kTag.c_str(), "Parallel benchmark finished in "
                                       << benchmark_result_->GetBenchmarkDuration().count() << " seconds.");
}

void BenchmarkRunner::WarmUpFunctions(const size_t repetition) {
  AWS_LOGSTREAM_INFO(kTag.c_str(), "Warming up functions for repetition " << repetition << "...");

  const auto& lambda_client = client_->GetLambdaClient();
  std::vector<Aws::Lambda::Model::InvokeOutcomeCallable> outcome_futures;
  outcome_futures.reserve(invoke_warmup_requests_.front().size());

  for (const auto& [invocation_id, invoke_request] : invoke_warmup_requests_.front()) {
    outcome_futures.emplace_back(lambda_client.InvokeCallable(invoke_request));
  }

  for (const auto& outcome_future : outcome_futures) {
    outcome_future.wait();
  }

  if (IsAsyncBenchmark()) {
    CollectSqsMessages(outcome_futures.size());
  }

  AWS_LOGSTREAM_INFO(kTag.c_str(), "Functions warmed up.");
}

std::pair<Aws::String, Aws::Lambda::Model::InvokeRequest> BenchmarkRunner::CreateInvokeRequest(
    const Aws::String& function_name, const Aws::String& invocation_id, const size_t repetition, const bool is_warmup,
    const std::shared_ptr<Aws::IOStream>& payload) {
  const auto invocation_type = IsAsyncBenchmark() ? Aws::Lambda::Model::InvocationType::Event
                                                  : Aws::Lambda::Model::InvocationType::RequestResponse;
  Aws::StringStream supplemented_id;
  supplemented_id << (config_->repetition_count_ > 1 ? "repetition-" + std::to_string(repetition) + "-" : "");
  supplemented_id << invocation_id << (is_warmup ? "-warmup" : "");

  const auto json_value = [&]() {
    Aws::Utils::Json::JsonValue value;

    if (payload) {
      value = Aws::Utils::Json::JsonValue(StreamToString(payload.get()));
    }

    return value.WithString("invocation_id", supplemented_id.str()).WithBool("is_warmup", is_warmup);
  }();

  const auto body = std::make_shared<Aws::StringStream>(json_value.View().WriteCompact());

  auto invoke_request = Aws::Lambda::Model::InvokeRequest()
                            .WithFunctionName(function_name)
                            .WithInvocationType(invocation_type)
                            .WithLogType(Aws::Lambda::Model::LogType::Tail);
  invoke_request.SetBody(body);
  invoke_request.SetContentType("application/json");

  return {supplemented_id.str(), invoke_request};
}

void BenchmarkRunner::CreateInvokeRequests(const bool is_warmup) {
  AWS_LOGSTREAM_INFO(kTag.c_str(), "Creating invoke requests" << (is_warmup ? "for function warm-up" : "") << "...");
  std::vector<std::unordered_map<Aws::String, Aws::Lambda::Model::InvokeRequest>> invoke_requests(
      config_->repetition_count_);

  for (size_t i = 0; i < config_->repetition_count_; i++) {
    invoke_requests[i].reserve(config_->concurrent_invocation_count_);

    for (const auto& config : config_->repetition_configs_[i]) {
      invoke_requests[i].emplace(
          CreateInvokeRequest(config.function_name, config.invocation_id, i, is_warmup, config.payload));
    }
  }

  if (is_warmup) {
    invoke_warmup_requests_ = invoke_requests;
  } else {
    invoke_requests_ = invoke_requests;
  }

  AWS_LOGSTREAM_INFO(kTag.c_str(), "Invoke requests " << (is_warmup ? "for function warm-up" : "") << " created.");
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
      const auto json_view = json_value.View();
      const auto invocation_id = json_view.GetObject("requestPayload").GetString("invocation_id");

      sqs_messages->emplace(invocation_id, message.GetBody());

      const auto delete_message_outcome = sqs_client.DeleteMessage(Aws::SQS::Model::DeleteMessageRequest()
                                                                       .WithQueueUrl(*sqs_queue_url_)
                                                                       .WithReceiptHandle(message.GetReceiptHandle()));

      if (!delete_message_outcome.IsSuccess()) {
        Fail(delete_message_outcome.GetError().GetMessage());
      }
    }
  }
  return sqs_messages;
}

Aws::Utils::CryptoBuffer BenchmarkRunner::OpenFunctionZip(const Aws::String& function_path) {
  std::ifstream infile(function_path, std::ios::in | std::ios::binary);

  if (!infile) {
    Fail(function_path + " could not be opened.");
  }

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

    if (!function_name_found) {
      Fail("S3 key could not be extracted from function name " + function_name + ".");
    }

    code.WithS3Bucket(function_path).WithS3Key(matches[1]);
  }

  return code;
}

std::vector<Aws::Lambda::Model::CreateFunctionOutcome> BenchmarkRunner::UploadFunctions(
    const size_t thread_count, const size_t thread_index, const Aws::Lambda::Model::TracingConfig& tracing_config) {
  const size_t function_count = config_->function_configs_.size();
  const size_t block_size = (function_count / thread_count) + (function_count % thread_count != 0 ? 1 : 0);

  std::vector<Aws::Lambda::Model::CreateFunctionOutcome> outcomes;
  outcomes.reserve(block_size);

  const auto& lambda_client = client_->GetLambdaClient();

  for (size_t i = thread_index; i < config_->function_configs_.size(); i += thread_count) {
    const auto& function_config = config_->function_configs_[i];
    const auto code =
        SetFunctionCode(function_config.function_path, function_config.function_name, function_config.is_local);

    AWS_LOGSTREAM_INFO(kTag.c_str(), "Creating function " << function_config.function_name << "...");

    const auto create_function_request = Aws::Lambda::Model::CreateFunctionRequest()
                                             .WithFunctionName(function_config.function_name)
                                             .WithRuntime(Aws::Lambda::Model::Runtime::provided_al2)
                                             .WithRole(function_role_arn_)
                                             .WithHandler("HandlerFunction")
                                             .WithCode(code)
                                             .WithTimeout(kLambdaFunctionTimeoutSeconds)
                                             .WithTracingConfig(tracing_config)
                                             .WithMemorySize(function_config.memory_size);

    outcomes.emplace_back(lambda_client.CreateFunction(create_function_request));
  }
  return outcomes;
}

bool BenchmarkRunner::IsWarmStartBenchmark() { return config_->warm_up_strategy_ != WarmUpStrategy::kNone; }

bool BenchmarkRunner::IsAsyncBenchmark() { return config_->use_event_queue_ != UseEventQueue::kNo; }

bool BenchmarkRunner::IsParallelBenchmark() { return config_->concurrent_invocation_count_ > 1; }

}  // namespace skyrise
