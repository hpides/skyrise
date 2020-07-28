#include "benchmark_runner.hpp"

#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <memory>

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/iam/model/GetRoleRequest.h>
#include <aws/lambda/LambdaClient.h>
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

namespace skyrise {

// Default location of certificate authority file on Amazon Linux 1
const std::string kCaFile = "/etc/pki/tls/certs/ca-bundle.crt";

BenchmarkRunner::BenchmarkRunner(const BenchmarkConfig& config) : config_(config) {
  Aws::Client::ClientConfiguration client_config;

  client_config.caFile = kCaFile;
  if (!std::ifstream(client_config.caFile).good()) {
    std::cout << "ERROR: AWS certificates are missing. Please provide " << kCaFile << ".\n";
    exit(1);
  }

  const auto credentials_provider = std::make_shared<Aws::Auth::EnvironmentAWSCredentialsProvider>();

  // TODO: Improve error handling; below AWS SDK call just checks for presence of AWS_ACCESS_KEY_ID
  if (credentials_provider == nullptr || (*credentials_provider).GetAWSCredentials().IsEmpty()) {
    std::cout << "ERROR: AWS credentials are missing. Please export AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.\n";
    exit(1);
  }

  std::cout << "\nCreating Client(s)...\n";
  iam_client_ = Aws::IAM::IAMClient(credentials_provider, client_config);
  lambda_client_ = Aws::Lambda::LambdaClient(credentials_provider, client_config);

  if (IsAsyncBenchmark()) {
    sqs_client_ = Aws::SQS::SQSClient(credentials_provider, client_config);
  }
}

void BenchmarkRunner::Run() {
  // TODO: Assert !result

  Setup();

  if (IsWarmStartBenchmark()) {
    WarmUpFunctions();
  }

  if (IsAsyncBenchmark() || IsParallelBenchmark()) {
    RunParallel();
  } else {
    RunSequential();
  }

  Teardown();
}

const std::shared_ptr<std::vector<BenchmarkItemResult>> BenchmarkRunner::GetBenchmarkResult() const {
  // TODO: Assert result != null
  return result_;
}

void BenchmarkRunner::Setup() {
  std::cout << "\nCreating Functions...\n";

  const auto get_role_outcome =
      iam_client_.GetRole(Aws::IAM::Model::GetRoleRequest().WithRoleName(config_.function_role_name_));
  // TODO: Assert success

  const auto role_arn = get_role_outcome.GetResult().GetRole().GetArn();

  std::vector<std::future<Aws::Lambda::Model::CreateFunctionOutcome>> create_function_outcomes;

  for (const auto& function_config : *config_.function_configs_) {
    std::cout << "Creating Function " << function_config.function_name << "...\n";

    create_function_outcomes.emplace_back(std::async([&]() {
      const auto create_function_request =
          Aws::Lambda::Model::CreateFunctionRequest()
              .WithFunctionName(function_config.function_name)
              .WithRuntime(Aws::Lambda::Model::Runtime::provided)
              .WithRole(role_arn)
              .WithHandler("HandlerFunction")
              .WithCode(Aws::Lambda::Model::FunctionCode().WithZipFile(OpenFunctionZip(function_config.function_path)))
              .WithTimeout(config_.timeout_)
              .WithMemorySize(function_config.memory_size);

      return lambda_client_.CreateFunction(create_function_request);
    }));
  }

  for (auto& outcome_future : create_function_outcomes) {
    const auto outcome = outcome_future.get();
    // TODO: Assert success

    if (outcome.IsSuccess()) {
      std::cout << outcome.GetResult().GetFunctionName() << " created.\n";
    }
  }

  if (IsAsyncBenchmark()) {
    SetupAsync();
  }

  std::cout << "\nCreating Invoke Requests...\n";

  if (IsWarmStartBenchmark()) {
    invoke_warmup_requests_ = CreateInvokeRequests(true);
  }

  invoke_requests_ = CreateInvokeRequests(false);
}

void BenchmarkRunner::SetupAsync() {
  const Aws::String queue_name = config_.benchmark_id_ + "-" + config_.benchmark_timestamp_;

  std::cout << "\nCreating Queue " << queue_name << "\n";

  const auto create_queue_outcome =
      sqs_client_.CreateQueue(Aws::SQS::Model::CreateQueueRequest().WithQueueName(queue_name));

  if (create_queue_outcome.IsSuccess()) {
    sqs_queue_url_ = std::make_shared<Aws::String>(create_queue_outcome.GetResult().GetQueueUrl());
    std::cout << "Queue " << create_queue_outcome.GetResult().GetQueueUrl() << " created.\n";
  } else {
    std::cout << "ERROR: CreateQueue failed due to the following error. "
              << create_queue_outcome.GetError().GetMessage() << ".\n";
    exit(1);
  }

  const auto queue_attributes_outcome =
      sqs_client_.GetQueueAttributes(Aws::SQS::Model::GetQueueAttributesRequest()
                                         .WithQueueUrl(*sqs_queue_url_)
                                         .WithAttributeNames(std::vector<Aws::SQS::Model::QueueAttributeName>(
                                             1, Aws::SQS::Model::QueueAttributeName::QueueArn)));

  // TODO: Assert success
  const auto queue_arn =
      queue_attributes_outcome.GetResult().GetAttributes().at(Aws::SQS::Model::QueueAttributeName::QueueArn);

  for (const auto& function_config : *config_.function_configs_) {
    lambda_client_.PutFunctionEventInvokeConfig(
        Aws::Lambda::Model::PutFunctionEventInvokeConfigRequest()
            .WithFunctionName(function_config.function_name)
            .WithDestinationConfig(Aws::Lambda::Model::DestinationConfig()
                                       .WithOnSuccess(Aws::Lambda::Model::OnSuccess().WithDestination(queue_arn))
                                       .WithOnFailure(Aws::Lambda::Model::OnFailure().WithDestination(queue_arn))));
  }
}

void BenchmarkRunner::Teardown() {
  std::cout << "\nDeleting Functions...\n";

  std::vector<std::pair<Aws::String, std::future<Aws::Lambda::Model::DeleteFunctionOutcome>>> delete_function_outcomes;

  for (const auto& function_config : *config_.function_configs_) {
    delete_function_outcomes.emplace_back(
        function_config.function_name, std::async([&]() {
          const auto delete_function_request =
              Aws::Lambda::Model::DeleteFunctionRequest().WithFunctionName(function_config.function_name);

          return lambda_client_.DeleteFunction(delete_function_request);
        }));
  }

  for (auto& outcome_pair : delete_function_outcomes) {
    const auto outcome = outcome_pair.second.get();

    if (outcome.IsSuccess()) {
      std::cout << "Function " << outcome_pair.first << " deleted.\n";
    } else {
      std::cout << outcome.GetError().GetMessage();
    }
  }

  if (sqs_queue_url_) {
    std::cout << "\nDeleting SQS Queue...\n";

    const auto outcome = sqs_client_.DeleteQueue(Aws::SQS::Model::DeleteQueueRequest().WithQueueUrl(*sqs_queue_url_));
    if (outcome.IsSuccess()) {
      std::cout << "Deleted Queue " << *sqs_queue_url_ << "\n";
    }
  }
}

void BenchmarkRunner::RunSequential() {
  std::cout << "\nInvoking Functions sequentially...\n";

  const auto benchmark_item_results = std::make_shared<std::vector<BenchmarkItemResult>>();

  // BENCHMARK STARTS
  const auto benchmark_start = std::chrono::steady_clock::now();

  for (const auto& [invocation_id, invoke_request] : *invoke_requests_) {
    benchmark_item_results->emplace_back(RunBenchmarkItem(invocation_id, invoke_request));
  }

  // BENCHMARK ENDS
  const auto benchmark_end = std::chrono::steady_clock::now();
  const auto benchmark_run_duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(benchmark_end - benchmark_start);

  WriteResult(benchmark_item_results, benchmark_run_duration);
}

void BenchmarkRunner::RunParallel() {
  std::cout << "\nInvoking Functions concurrently...\n";

  std::vector<std::future<BenchmarkItemResult>> future_results;
  const auto benchmark_item_results = std::make_shared<std::vector<BenchmarkItemResult>>();

  // BENCHMARK STARTS
  const auto benchmark_start = std::chrono::steady_clock::now();

  for (const auto& [invocation_id, invoke_request] : *invoke_requests_) {
    future_results.emplace_back(std::async(&BenchmarkRunner::RunBenchmarkItem, this, invocation_id, invoke_request));
  }

  for (auto& result : future_results) {
    benchmark_item_results->emplace_back(result.get());
  }

  // BENCHMARK ENDS
  const auto benchmark_end = std::chrono::steady_clock::now();
  const auto benchmark_run_duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(benchmark_end - benchmark_start);

  WriteResult(benchmark_item_results, benchmark_run_duration);
}

void BenchmarkRunner::WarmUpFunctions() {
  std::cout << "\nWarming up Functions...\n";

  std::vector<std::future<BenchmarkItemResult>> future_results;

  // TODO: Do not run every invoke request if not necessary
  for (const auto& [invocation_id, invoke_request] : *invoke_warmup_requests_) {
    future_results.emplace_back(std::async(&BenchmarkRunner::RunBenchmarkItem, this, invocation_id, invoke_request));
  }

  for (auto& result : future_results) {
    result.get();
  }

  if (IsAsyncBenchmark()) {
    CollectSqsMessages(future_results.size());
  }
}

std::shared_ptr<std::map<Aws::String, Aws::Lambda::Model::InvokeRequest>> BenchmarkRunner::CreateInvokeRequests(
    const bool with_warm_up_suffix) {
  const auto invoke_requests = std::make_shared<std::map<Aws::String, Aws::Lambda::Model::InvokeRequest>>();

  const auto invocation_type = IsAsyncBenchmark() ? Aws::Lambda::Model::InvocationType::Event
                                                  : Aws::Lambda::Model::InvocationType::RequestResponse;

  for (const auto& config : *(config_.invocation_configs_)) {
    auto invoke_request = Aws::Lambda::Model::InvokeRequest()
                              .WithFunctionName(config.function_name)
                              .WithInvocationType(invocation_type)
                              .WithLogType(Aws::Lambda::Model::LogType::Tail);

    const Aws::String invocation_id = with_warm_up_suffix ? (config.invocation_id + "-warmup") : config.invocation_id;

    const auto json_value = Aws::Utils::Json::JsonValue(*config.payload).WithString("invocationID", invocation_id);
    const auto json_view = json_value.View();
    const auto body = std::make_shared<Aws::StringStream>(json_view.WriteCompact());

    invoke_request.SetBody(body);
    invoke_request.SetContentType("application/json");

    invoke_requests->emplace(std::make_pair(invocation_id, invoke_request));
  }
  return invoke_requests;
}

std::shared_ptr<std::map<Aws::String, Aws::String>> BenchmarkRunner::CollectSqsMessages(const size_t num_invocations) {
  const auto sqs_messages = std::make_shared<std::map<Aws::String, Aws::String>>();

  size_t receive_message_requests = 0;

  while (sqs_messages->size() < num_invocations && receive_message_requests < config_.timeout_) {
    receive_message_requests++;
    const auto receive_message_outcome = sqs_client_.ReceiveMessage(Aws::SQS::Model::ReceiveMessageRequest()
                                                                        .WithQueueUrl(*sqs_queue_url_)
                                                                        .WithWaitTimeSeconds(1)
                                                                        .WithMaxNumberOfMessages(10));
    const auto messages = receive_message_outcome.GetResult().GetMessages();

    for (const auto& message : messages) {
      const auto json_value = Aws::Utils::Json::JsonValue(message.GetBody());
      const auto json_view = json_value.View();
      const auto invocation_id = json_view.GetObject("requestPayload").GetString("invocationID");

      sqs_messages->emplace(std::make_pair(invocation_id, message.GetBody()));

      sqs_client_.DeleteMessage(Aws::SQS::Model::DeleteMessageRequest()
                                    .WithQueueUrl(*sqs_queue_url_)
                                    .WithReceiptHandle(message.GetReceiptHandle()));
      // TODO: Assert success
    }
  }
  return sqs_messages;
}

Aws::Utils::CryptoBuffer BenchmarkRunner::OpenFunctionZip(const Aws::String& function_path) {
  std::ifstream infile;
  infile.open(function_path, std::ios::binary);

  if (infile.is_open()) {
    std::vector<char> buffer;

    while (!infile.eof()) {
      buffer.emplace_back(static_cast<char>(infile.get()));
    }
    infile.close();
    std::string ret(buffer.begin(), buffer.end() - 1);

    return Aws::Utils::CryptoBuffer((unsigned const char*)ret.c_str(), ret.size());
  } else {
    // TODO Fail
    std::cout << "File could not be opened\n";
    return Aws::Utils::CryptoBuffer();
  }
}

BenchmarkItemResult BenchmarkRunner::RunBenchmarkItem(const Aws::String& invocation_id,
                                                      const Aws::Lambda::Model::InvokeRequest& invoke_request) {
  const auto benchmark_item_start = std::chrono::steady_clock::now();

  const auto lambda_outcome =
      std::make_shared<Aws::Lambda::Model::InvokeOutcome>(lambda_client_.Invoke(invoke_request));

  const auto benchmark_item_end = std::chrono::steady_clock::now();

  return BenchmarkItemResult{
      invocation_id,  invoke_request, lambda_outcome->IsSuccess(), benchmark_item_start, benchmark_item_end,
      lambda_outcome, nullptr};
}

void BenchmarkRunner::WriteResult(const std::shared_ptr<std::vector<BenchmarkItemResult>> benchmark_item_results,
                                  const std::chrono::duration<size_t, std::milli> benchmark_run_duration) {
  if (IsAsyncBenchmark()) {
    sqs_messages_ = CollectSqsMessages(benchmark_item_results->size());

    for (auto& result : *benchmark_item_results) {
      result.sqs_message_body = std::make_shared<Aws::String>(sqs_messages_->at(result.invocation_id));
    }
  }

  std::cout << "\nTotal benchmark run duration: " << benchmark_run_duration.count() << " ms\n";

  result_ = benchmark_item_results;
}

bool BenchmarkRunner::IsWarmStartBenchmark() {
  return config_.execute_mode_ == ExecuteMode::WarmAsync || config_.execute_mode_ == ExecuteMode::WarmParallel ||
         config_.execute_mode_ == ExecuteMode::WarmSequential;
}

bool BenchmarkRunner::IsAsyncBenchmark() {
  return config_.execute_mode_ == ExecuteMode::ColdAsync || config_.execute_mode_ == ExecuteMode::WarmAsync;
}

bool BenchmarkRunner::IsParallelBenchmark() {
  return config_.execute_mode_ == ExecuteMode::ColdParallel || config_.execute_mode_ == ExecuteMode::WarmParallel;
}

}  // namespace skyrise
