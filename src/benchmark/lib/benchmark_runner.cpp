#include "benchmark_runner.hpp"

#include <fstream>
#include <functional>
#include <future>
#include <iostream>
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

#include "utils/assert.hpp"

namespace skyrise {

// TODO(anyone): Add commit hash to logging tag
const std::string kTag = "SKYRISE/BENCHMARK/BENCHMARK_RUNNER";

BenchmarkRunner::BenchmarkRunner(std::shared_ptr<ClientAws> client_aws) : client_aws_(std::move(client_aws)) {
  const auto get_role_outcome =
      client_aws_->GetIAMClient().GetRole(Aws::IAM::Model::GetRoleRequest().WithRoleName(kFunctionRoleName));

  if (!get_role_outcome.IsSuccess()) {
    Fail(get_role_outcome.GetError().GetMessage());
  }

  function_role_arn_ = get_role_outcome.GetResult().GetRole().GetArn();
}

std::shared_ptr<std::vector<BenchmarkItemResult>> BenchmarkRunner::RunConfig(const BenchmarkConfig& config) {
  try {
    SetConfig(config);

    Setup();

    if (IsWarmStartBenchmark()) {
      // TODO(anyone): Warm up functions prior to every repetition
      WarmUpFunctions();
    }

    if (IsAsyncBenchmark() || IsParallelBenchmark()) {
      RunParallel();
    } else {
      RunSequential();
    }
  } catch (const std::exception& e) {
    AWS_LOGSTREAM_ERROR(kTag.c_str(), e.what());
    Teardown();
    return nullptr;
  }

  Teardown();

  return std::move(result_);
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

  size_t num_functions = config_->function_configs_->size();
  size_t num_threads = num_functions > num_setup_threads_ ? num_setup_threads_ : num_functions;
  std::vector<std::future<std::vector<Aws::Lambda::Model::CreateFunctionOutcome>>> outcome_vec_futures;
  outcome_vec_futures.reserve(num_threads);

  for (size_t i = 0; i < num_threads; i++) {
    outcome_vec_futures.emplace_back(std::async(&BenchmarkRunner::UploadFunctions, this, num_threads, i));
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
    invoke_warmup_requests_ = CreateWarmupInvokeRequests();
  }

  invoke_requests_ = CreateInvokeRequests();
}

void BenchmarkRunner::SetupAsync() {
  const Aws::String queue_name = config_->benchmark_id_ + "-" + config_->benchmark_timestamp_;

  AWS_LOGSTREAM_INFO(kTag.c_str(), "Creating queue " << queue_name << "...");

  const auto& sqs_client = client_aws_->GetSQSClient();
  const auto& lambda_client = client_aws_->GetLambdaClient();

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

  for (const auto& function_config : *config_->function_configs_) {
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
  delete_function_outcomes.reserve(config_->function_configs_->size());

  const auto& lambda_client = client_aws_->GetLambdaClient();

  for (const auto& function_config : *config_->function_configs_) {
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
        client_aws_->GetSQSClient().DeleteQueue(Aws::SQS::Model::DeleteQueueRequest().WithQueueUrl(*sqs_queue_url_));
    if (outcome.IsSuccess()) {
      AWS_LOGSTREAM_INFO(kTag.c_str(), "Queue " << *sqs_queue_url_ << " deleted.");
    } else {
      AWS_LOGSTREAM_ERROR(kTag.c_str(), outcome.GetError().GetMessage());
    }

    sqs_queue_url_.reset();
  }
}

void BenchmarkRunner::RunSequential() {
  AWS_LOGSTREAM_INFO(kTag.c_str(), "Invoking functions sequentially...");

  const auto benchmark_item_results = std::make_shared<std::vector<BenchmarkItemResult>>();
  benchmark_item_results->reserve(invoke_requests_->size());

  // BENCHMARK STARTS
  const auto benchmark_start = std::chrono::steady_clock::now();

  size_t invocation_index = 0;
  for (const auto& [invocation_id, invoke_request] : *invoke_requests_) {
    benchmark_item_results->emplace_back(RunBenchmarkItem(invocation_id, invoke_request));

    const auto is_last_invocation_of_repetition = (invocation_index + 1) % config_->num_invocations_ == 0;
    if (config_->num_repetitions_ > 1 && is_last_invocation_of_repetition) {
      const size_t current_repetition = (invocation_index / config_->num_invocations_);
      AWS_LOGSTREAM_INFO(kTag.c_str(), "Repetition " << current_repetition << " completed.");
      config_->after_repetition_callbacks_[current_repetition]();
    }
    invocation_index++;
  }

  // BENCHMARK ENDS
  const auto benchmark_end = std::chrono::steady_clock::now();
  const auto benchmark_run_duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(benchmark_end - benchmark_start);

  AWS_LOGSTREAM_INFO(kTag.c_str(), "Functions invoked sequentially.");

  WriteResult(benchmark_item_results, benchmark_run_duration);
}

void BenchmarkRunner::RunParallel() {
  AWS_LOGSTREAM_INFO(kTag.c_str(), "Invoking functions concurrently...");

  std::vector<std::future<BenchmarkItemResult>> future_results;
  future_results.reserve(invoke_requests_->size());

  const auto benchmark_item_results = std::make_shared<std::vector<BenchmarkItemResult>>();
  benchmark_item_results->reserve(invoke_requests_->size());

  // BENCHMARK STARTS
  const auto benchmark_start = std::chrono::steady_clock::now();

  size_t invocation_index = 0;
  for (const auto& [invocation_id, invoke_request] : *invoke_requests_) {
    future_results.emplace_back(std::async(&BenchmarkRunner::RunBenchmarkItem, this, invocation_id, invoke_request));

    const auto is_last_invocation_of_repetition = (invocation_index + 1) % config_->num_invocations_ == 0;
    if (config_->num_repetitions_ > 1 && is_last_invocation_of_repetition) {
      const auto current_repetition = (invocation_index / config_->num_invocations_);
      AWS_LOGSTREAM_INFO(kTag.c_str(), "Repetition " << current_repetition << " dispatched.");
      config_->after_repetition_callbacks_[current_repetition]();
    }
    invocation_index++;
  }

  for (auto& result : future_results) {
    benchmark_item_results->emplace_back(result.get());
  }

  // BENCHMARK ENDS
  const auto benchmark_end = std::chrono::steady_clock::now();
  const auto benchmark_run_duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(benchmark_end - benchmark_start);

  AWS_LOGSTREAM_INFO(kTag.c_str(), "Functions invoked concurrently.");

  WriteResult(benchmark_item_results, benchmark_run_duration);
}

void BenchmarkRunner::WarmUpFunctions() {
  AWS_LOGSTREAM_INFO(kTag.c_str(), "Warming up functions...");

  std::vector<std::future<BenchmarkItemResult>> future_results;
  future_results.reserve(invoke_warmup_requests_->size());

  for (const auto& [invocation_id, invoke_request] : *invoke_warmup_requests_) {
    future_results.emplace_back(std::async(&BenchmarkRunner::RunBenchmarkItem, this, invocation_id, invoke_request));
  }

  for (auto& result : future_results) {
    result.get();
  }

  if (IsAsyncBenchmark()) {
    CollectSqsMessages(future_results.size());
  }

  AWS_LOGSTREAM_INFO(kTag.c_str(), "Functions warmed up.");
}

std::pair<Aws::String, Aws::Lambda::Model::InvokeRequest> BenchmarkRunner::CreateInvokeRequest(
    const Aws::String& function_name, const Aws::String& invocation_id, const size_t repetition, const bool is_warmup,
    const std::shared_ptr<Aws::IOStream>& payload) {
  const auto invocation_type = IsAsyncBenchmark() ? Aws::Lambda::Model::InvocationType::Event
                                                  : Aws::Lambda::Model::InvocationType::RequestResponse;
  Aws::StringStream supplemented_id;
  supplemented_id << (config_->num_repetitions_ > 1 ? "repetition-" + std::to_string(repetition) + "-" : "");
  supplemented_id << invocation_id << (is_warmup ? "-warmup" : "");

  const auto json_value = [&]() {
    Aws::Utils::Json::JsonValue value;

    if (payload) {
      value = Aws::Utils::Json::JsonValue(StreamToString(&*payload));
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

std::shared_ptr<std::unordered_map<Aws::String, Aws::Lambda::Model::InvokeRequest>>
BenchmarkRunner::CreateInvokeRequests() {
  AWS_LOGSTREAM_INFO(kTag.c_str(), "Creating invoke requests...");

  auto invoke_requests = std::make_shared<std::unordered_map<Aws::String, Aws::Lambda::Model::InvokeRequest>>();
  invoke_requests->reserve(config_->num_repetitions_ * config_->invocation_configs_->size());

  for (size_t i = 0; i < config_->num_repetitions_; i++) {
    for (const auto& config : *(config_->invocation_configs_)) {
      invoke_requests->emplace(
          CreateInvokeRequest(config.function_name, config.invocation_id, i, false, config.payload));
    }
  }

  AWS_LOGSTREAM_INFO(kTag.c_str(), "Invoke requests created.");

  return invoke_requests;
}

std::shared_ptr<std::unordered_map<Aws::String, Aws::Lambda::Model::InvokeRequest>>
BenchmarkRunner::CreateWarmupInvokeRequests() {
  AWS_LOGSTREAM_INFO(kTag.c_str(), "Creating invoke requests for function warm-up...");

  auto invoke_requests = std::make_shared<std::unordered_map<Aws::String, Aws::Lambda::Model::InvokeRequest>>();

  if (config_->execute_mode_ == ExecuteMode::WarmSequential) {
    invoke_requests->reserve(config_->num_repetitions_ * config_->function_configs_->size());

    for (const auto& function_config : *config_->function_configs_) {
      invoke_requests->emplace(
          CreateInvokeRequest(function_config.function_name, function_config.function_name, 0, true));
    }
  } else {
    invoke_requests->reserve(config_->num_repetitions_ * config_->invocation_configs_->size());

    for (const auto& invocation_config : *config_->invocation_configs_) {
      invoke_requests->emplace(
          CreateInvokeRequest(invocation_config.function_name, invocation_config.invocation_id, 0, true));
    }
  }

  AWS_LOGSTREAM_INFO(kTag.c_str(), "Invoke requests for function warm-up created.");

  return invoke_requests;
}

std::shared_ptr<std::unordered_map<Aws::String, Aws::String>> BenchmarkRunner::CollectSqsMessages(
    const size_t num_invocations) {
  auto sqs_messages = std::make_shared<std::unordered_map<Aws::String, Aws::String>>();
  sqs_messages->reserve(num_invocations);

  const auto& sqs_client = client_aws_->GetSQSClient();
  size_t receive_message_requests = 0;

  while (sqs_messages->size() < num_invocations && receive_message_requests < config_->timeout_) {
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
  std::ifstream infile;
  infile.open(function_path, std::ios::binary);

  if (!infile.is_open()) {
    Fail(function_path + " could not be opened.");
  }

  std::vector<char> buffer;

  while (!infile.eof()) {
    buffer.emplace_back(static_cast<char>(infile.get()));
  }
  infile.close();
  std::string ret(buffer.begin(), buffer.end() - 1);

  return Aws::Utils::CryptoBuffer(reinterpret_cast<const unsigned char*>(ret.c_str()), ret.size());
}

std::vector<Aws::Lambda::Model::CreateFunctionOutcome> BenchmarkRunner::UploadFunctions(const size_t num_threads,
                                                                                        const size_t thread_index) {
  size_t num_functions = config_->function_configs_->size();
  const double block_size = num_functions / static_cast<double>(num_threads);
  const auto lower_bound = static_cast<size_t>(thread_index * block_size);
  const auto upper_bound = static_cast<size_t>(static_cast<double>(thread_index + 1) * block_size);

  std::vector<Aws::Lambda::Model::CreateFunctionOutcome> outcomes;
  outcomes.reserve(upper_bound - lower_bound);

  const auto& lambda_client = client_aws_->GetLambdaClient();

  for (size_t i = lower_bound; i < upper_bound; i++) {
    const auto& function_config = (*config_->function_configs_)[i];

    AWS_LOGSTREAM_INFO(kTag.c_str(), "Creating function " << function_config.function_name << "...");

    const auto create_function_request =
        Aws::Lambda::Model::CreateFunctionRequest()
            .WithFunctionName(function_config.function_name)
            .WithRuntime(Aws::Lambda::Model::Runtime::provided_al2)
            .WithRole(function_role_arn_)
            .WithHandler("HandlerFunction")
            .WithCode(Aws::Lambda::Model::FunctionCode().WithZipFile(OpenFunctionZip(function_config.function_path)))
            .WithTimeout(config_->timeout_)
            .WithMemorySize(function_config.memory_size);

    outcomes.emplace_back(lambda_client.CreateFunction(create_function_request));
  }
  return outcomes;
}

BenchmarkItemResult BenchmarkRunner::RunBenchmarkItem(const Aws::String& invocation_id,
                                                      const Aws::Lambda::Model::InvokeRequest& invoke_request) {
  const auto benchmark_item_start = std::chrono::steady_clock::now();

  auto lambda_outcome = client_aws_->GetLambdaClient().Invoke(invoke_request);

  const auto benchmark_item_end = std::chrono::steady_clock::now();

  const auto lambda_result =
      std::make_shared<Aws::Lambda::Model::InvokeResult>(lambda_outcome.GetResultWithOwnership());

  return BenchmarkItemResult{invocation_id,
                             invoke_request,
                             lambda_outcome.IsSuccess(),
                             benchmark_item_start,
                             benchmark_item_end,
                             lambda_result,
                             {}};
}

void BenchmarkRunner::WriteResult(const std::shared_ptr<std::vector<BenchmarkItemResult>>& benchmark_item_results,
                                  const std::chrono::duration<size_t, std::milli> benchmark_run_duration) {
  if (IsAsyncBenchmark()) {
    sqs_messages_ = CollectSqsMessages(benchmark_item_results->size());

    for (auto& result : *benchmark_item_results) {
      result.sqs_message_body = sqs_messages_->at(result.invocation_id);
    }
  }

  AWS_LOGSTREAM_INFO(kTag.c_str(), "Total benchmark run duration: " << benchmark_run_duration.count() << " ms");

  result_ = benchmark_item_results;
}

bool BenchmarkRunner::IsWarmStartBenchmark() {
  return config_->execute_mode_ == ExecuteMode::WarmAsync || config_->execute_mode_ == ExecuteMode::WarmParallel ||
         config_->execute_mode_ == ExecuteMode::WarmSequential;
}

bool BenchmarkRunner::IsAsyncBenchmark() {
  return config_->execute_mode_ == ExecuteMode::ColdAsync || config_->execute_mode_ == ExecuteMode::WarmAsync;
}

bool BenchmarkRunner::IsParallelBenchmark() {
  return config_->execute_mode_ == ExecuteMode::ColdParallel || config_->execute_mode_ == ExecuteMode::WarmParallel;
}

}  // namespace skyrise
