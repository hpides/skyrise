#include "scheduler/coordinator/lambda_executor.hpp"

#include <cmath>
#include <thread>

#include <aws/core/utils/base64/Base64.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/sqs/model/DeleteMessageRequest.h>
#include <aws/sqs/model/DeleteQueueRequest.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>

#include "client/base_client.hpp"
#include "compiler/physical_query_plan/pqp_serialization.hpp"
#include "configuration.hpp"
#include "constants.hpp"
#include "utils/compression.hpp"

namespace skyrise {

LambdaExecutor::LambdaExecutor(const std::shared_ptr<const BaseClient>& client, const std::string& worker_function_name)
    : client_(client), worker_function_name_(worker_function_name) {
  SetupSqsResponseQueue();
}

LambdaExecutor::~LambdaExecutor() {
  const auto request = Aws::SQS::Model::DeleteQueueRequest().WithQueueUrl(sqs_response_queue_url_);
  client_->GetSqsClient()->DeleteQueue(request);
}

void LambdaExecutor::Execute(
    const std::shared_ptr<PqpPipeline>& pqp_pipeline,
    const std::function<void(std::shared_ptr<PqpPipelineFragmentExecutionResult>)>& on_finished_callback) {
  Aws::Lambda::Model::InvokeRequest invoke_request;
  invoke_request.WithFunctionName(worker_function_name_).WithInvocationType(Aws::Lambda::Model::InvocationType::Event);

  const std::string serialized_pqp_pipeline_template =
      SerializePqp(pqp_pipeline->PqpPipelineFragmentTemplate()->TemplatedPlan());
  auto request_body = Aws::Utils::Json::JsonValue().WithString(kWorkerRequestPqpPipelineTemplateAttribute,
                                                               serialized_pqp_pipeline_template);

  const std::vector<PipelineFragmentDefinition>& fragment_definitions = pqp_pipeline->FragmentDefinitions();
  const size_t fragment_count = fragment_definitions.size();
  const size_t fragments_per_worker_count =
      std::ceil(fragment_count / static_cast<double>(kWorkerRecursiveInvocationThreshold));

  const std::string pipeline_id = pqp_pipeline->Identity();
  const std::string worker_id_prefix = pipeline_id + "-worker";
  size_t worker_count = 0;

  for (size_t i = 0; i < fragment_count; i += fragments_per_worker_count) {
    Aws::Utils::Array<Aws::Utils::Json::JsonValue> fragment_array(fragments_per_worker_count);
    for (size_t j = 0; j < fragments_per_worker_count && (j + i) < fragment_count; ++j) {
      fragment_array.GetItem(j) = Aws::Utils::Json::JsonValue().WithObject(kWorkerRequestFragmentDefinitionAttribute,
                                                                           fragment_definitions[i + j].ToJson());
    }
    request_body.WithArray(kWorkerRequestFragmentDefinitionsAttribute, fragment_array);

    const std::string worker_id = worker_id_prefix + std::to_string(worker_count);
    request_body.WithString(kWorkerRequestIdAttribute, worker_id)
        .WithString(kRequestResponseQueueUrlAttribute, sqs_response_queue_url_);

    std::string payload = request_body.View().WriteCompact();
    if constexpr (kLambdaFunctionInvocationPayloadCompression) {
      auto compressed_json =
          Aws::Utils::Json::JsonValue()
              .WithString(kRequestCompressedAttribute, Aws::Utils::Base64::Base64().Encode(Compress(payload)))
              .WithInt64(kRequestDecompressedSizeAttribute, payload.size());
      payload = compressed_json.View().WriteCompact();
    }

    const auto request_stream = std::make_shared<Aws::StringStream>(payload);
    invoke_request.SetBody(request_stream);

    const auto lambda_client = client_->GetLambdaClient();
    lambda_client->InvokeAsync(
        invoke_request,
        [&](const Aws::Lambda::LambdaClient* /*client*/, const Aws::Lambda::Model::InvokeRequest& /*request*/,
            const Aws::Lambda::Model::InvokeOutcome& outcome,
            const std::shared_ptr<const Aws::Client::AsyncCallerContext>& /*context*/) {
          if (!outcome.IsSuccess()) {
            std::cerr << "Error invoking function: " << outcome.GetError().GetMessage() << "\n";
          }
        });

    ++worker_count;
  }

  collector_threads_.emplace_back(CollectSqsMessages, client_->GetSqsClient(), sqs_response_queue_url_, pipeline_id,
                                  fragment_count, on_finished_callback);
  collector_threads_.back().detach();
};

void LambdaExecutor::SetupSqsResponseQueue() {
  const std::string queue_name = std::string(kRequestResponseQueueNamePrefix) + RandomString(8);
  Aws::SQS::Model::CreateQueueRequest request;
  request.WithQueueName(queue_name);
  const auto outcome = client_->GetSqsClient()->CreateQueue(request);
  Assert(outcome.IsSuccess(), outcome.GetError().GetMessage());
  sqs_response_queue_url_ = outcome.GetResult().GetQueueUrl();
}

void LambdaExecutor::CollectSqsMessages(
    const std::shared_ptr<const Aws::SQS::SQSClient> sqs_client, const std::string sqs_response_queue_url,
    const std::string pipeline_id, const size_t invocation_count,
    const std::function<void(std::shared_ptr<PqpPipelineFragmentExecutionResult>)> on_finished_callback) {
  size_t sqs_message_count = 0;

  const auto receive_message_request = Aws::SQS::Model::ReceiveMessageRequest()
                                           .WithQueueUrl(sqs_response_queue_url)
                                           .WithMaxNumberOfMessages(1)
                                           .WithWaitTimeSeconds(20);

  std::vector<std::thread> threads;

  auto handle_message = [&](const Aws::Vector<Aws::SQS::Model::Message>& messages) {
    for (const auto& message : messages) {
      const auto response = Aws::Utils::Json::JsonValue(message.GetBody());
      const auto response_view = response.View();
      // We set the result to successful, as we received a message.
      const auto fragment_result =
          std::make_shared<PqpPipelineFragmentExecutionResult>(PqpPipelineFragmentExecutionResult{
              .pipeline_id = pipeline_id,
              .worker_id = response_view.GetString(kWorkerResponseIdAttribute),
              .is_success = response_view.GetInteger(kResponseIsSuccessAttribute) != 0,
              .message = response_view.GetString(kResponseMessageAttribute),
              .runtime_ms = (size_t)response_view.GetInteger(kWorkerResponseRuntimeMsAttribute),
              .function_instance_size_mb = (size_t)response_view.GetInt64(kWorkerResponseMemorySizeMbAttribute),
              .export_data_size_bytes = (size_t)response_view.GetInteger(kWorkerResponseExportDataSizeBytesAttribute),
              .metering = response_view.GetObject(kResponseMeteringAttribute).Materialize()});

      const auto delete_message_outcome = sqs_client->DeleteMessage(Aws::SQS::Model::DeleteMessageRequest()
                                                                        .WithQueueUrl(sqs_response_queue_url)
                                                                        .WithReceiptHandle(message.GetReceiptHandle()));
      Assert(delete_message_outcome.IsSuccess(), delete_message_outcome.GetError().GetMessage());
      on_finished_callback(fragment_result);
    }
  };

  while (sqs_message_count < invocation_count) {
    const auto receive_message_outcome = sqs_client->ReceiveMessage(receive_message_request);
    const auto messages = receive_message_outcome.GetResult().GetMessages();
    sqs_message_count += messages.size();
    threads.emplace_back(handle_message, messages);
  }

  for (auto& thread : threads) {
    thread.join();
  }
}

}  // namespace skyrise
