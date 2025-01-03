#include "worker_function.hpp"

#include <chrono>

#include <aws/lambda/model/InvokeRequest.h>

#include "client/base_client.hpp"
#include "compiler/physical_query_plan/pqp_pipeline.hpp"
#include "compiler/physical_query_plan/pqp_serialization.hpp"
#include "configuration.hpp"
#include "constants.hpp"
#include "operator/execution_context.hpp"
#include "operator/export_operator.hpp"
#include "scheduler/worker/fragment_scheduler.hpp"
#include "scheduler/worker/operator_task.hpp"
#include "storage/backend/s3_storage.hpp"
#include "utils/profiling/function_host_information.hpp"

namespace skyrise {

bool WorkerFunction::ValidateRequest(const Aws::Utils::Json::JsonView& request) {
  return request.KeyExists(kWorkerRequestIdAttribute) &&
         request.KeyExists(kWorkerRequestPqpPipelineTemplateAttribute) &&
         request.KeyExists(kWorkerRequestFragmentDefinitionsAttribute) &&
         request.GetArray(kWorkerRequestFragmentDefinitionsAttribute).GetLength() > 0;
}

void WorkerFunction::InvokeRecursive(const Aws::Utils::Json::JsonView& request,
                                     const std::shared_ptr<skyrise::BaseClient>& client) {
  Assert(request.KeyExists(kRequestResponseQueueUrlAttribute),
         "Recursive worker invocation requires an SQS queue to process responses.");

  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  const char* worker_function_name = std::getenv("AWS_LAMBDA_FUNCTION_NAME");
  std::string worker_id = request.GetString(kWorkerRequestIdAttribute);

  const auto lambda_client = client->GetLambdaClient();
  Aws::Lambda::Model::InvokeRequest invoke_request;
  invoke_request.WithFunctionName(worker_function_name)
      .WithInvocationType(Aws::Lambda::Model::InvocationType::Event)
      .SetLogType(Aws::Lambda::Model::LogType::None);

  auto new_invoke_payload = request.Materialize();
  Aws::Utils::Array<Aws::Utils::Json::JsonValue> fragment_array(1);

  const auto fragment_definitions = request.GetArray(kWorkerRequestFragmentDefinitionsAttribute);
  AWS_LOGSTREAM_DEBUG(kWorkerTag.c_str(), std::string("Recursively invoke ") +
                                              std::to_string(fragment_definitions.GetLength() - 1) +
                                              std::string(" workers."));

  // Start with the second definition, as the first one is assigned to this worker.
  for (size_t i = 1; i < fragment_definitions.GetLength(); i++) {
    new_invoke_payload.WithString(kWorkerRequestIdAttribute, worker_id + "-" + std::to_string(i));
    fragment_array[0] = fragment_definitions.GetItem(i).Materialize();
    new_invoke_payload.WithArray(kWorkerRequestFragmentDefinitionsAttribute, fragment_array);

    const auto payload_stream = std::make_shared<std::stringstream>(new_invoke_payload.View().WriteCompact());
    invoke_request.SetBody(payload_stream);

    lambda_client->InvokeAsync(
        invoke_request,
        [&](const Aws::Lambda::LambdaClient* /*client*/, const Aws::Lambda::Model::InvokeRequest& /*request*/,
            const Aws::Lambda::Model::InvokeOutcome& outcome,
            const std::shared_ptr<const Aws::Client::AsyncCallerContext>& /*context*/) {
          if (!outcome.IsSuccess()) {
          }
        });
  }
}

aws::lambda_runtime::invocation_response WorkerFunction::OnHandleRequest(
    const Aws::Utils::Json::JsonView& request) const {
  const auto runtime_start = std::chrono::steady_clock::now();

  AWS_LOGSTREAM_DEBUG(kWorkerTag.c_str(), std::string("Worker request: ") + request.WriteCompact());
  if (!ValidateRequest(request)) {
    return aws::lambda_runtime::invocation_response::failure("Invalid parameters provided.", "text/plain");
  }

  std::string worker_id = request.GetString(kWorkerRequestIdAttribute);
  size_t export_data_size_bytes = 0;

  try {
    const auto client = std::make_shared<BaseClient>();
    const auto fragment_definitions = request.GetArray(kWorkerRequestFragmentDefinitionsAttribute);
    if (fragment_definitions.GetLength() > 1) {
      InvokeRecursive(request, client);
    }

    const auto s3_client = client->GetS3Client();
    // Get pipeline fragment scheduler and schedule the root operator instance.
    std::shared_ptr<FragmentScheduler> scheduler = std::make_shared<FragmentScheduler>();
    std::shared_ptr<OperatorExecutionContext> context = std::make_shared<OperatorExecutionContext>(
        nullptr,
        [&s3_client](const std::string& storage_name) -> std::shared_ptr<Storage> {
          return std::make_shared<S3Storage>(s3_client, storage_name);
        },
        [&scheduler]() { return scheduler; });

    const std::string serialized_pqp_pipeline = request.GetString(kWorkerRequestPqpPipelineTemplateAttribute);
    std::shared_ptr<skyrise::AbstractOperatorProxy> pqp_pipeline = DeserializePqp(serialized_pqp_pipeline);
    Assert(pqp_pipeline != nullptr, "Unable to deserialize the PQP pipeline.");
    const auto fragment_template = std::make_shared<PipelineFragmentTemplate>(pqp_pipeline);

    const Aws::Utils::Json::JsonView serialized_fragment =
        fragment_definitions.GetItem(0).GetObject(kWorkerRequestFragmentDefinitionAttribute);
    const auto fragment_definition = PipelineFragmentDefinition::FromJson(serialized_fragment);

    const auto executable_fragment = fragment_template->GenerateFragmentPlan(fragment_definition);
    const auto operator_tree = executable_fragment->GetOrCreateOperatorInstance();
    const auto operator_task = OperatorTask::GenerateTasksFromOperator(operator_tree, context).first;
    AWS_LOGSTREAM_INFO(kWorkerTag.c_str(), "Starting execution.");
    scheduler->ScheduleAndWaitForTasks(operator_task);
    AWS_LOGSTREAM_INFO(kWorkerTag.c_str(), "Finished execution.");
    export_data_size_bytes = std::static_pointer_cast<ExportOperator>(operator_tree)->GetBytesWritten();
  } catch (const std::exception& exception) {
    AWS_LOGSTREAM_INFO(kWorkerTag.c_str(), exception.what());
  }

  const size_t runtime_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - runtime_start).count();

  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  const char* memory_size_mb_string = std::getenv("AWS_LAMBDA_FUNCTION_MEMORY_SIZE");
  const size_t memory_size_mb = memory_size_mb_string != nullptr ? std::stoull(memory_size_mb_string, nullptr, 10) : 0;

  Aws::Utils::Json::JsonValue response;
  response.WithInteger(kResponseIsSuccessAttribute, 1)
      .WithString(kResponseMessageAttribute, "Worker executed successfully.")
      .WithString(kWorkerResponseIdAttribute, worker_id)
      .WithInteger(kWorkerResponseRuntimeMsAttribute, runtime_ms)
      .WithInteger(kWorkerResponseMemorySizeMbAttribute, memory_size_mb)
      .WithInt64(kWorkerResponseExportDataSizeBytesAttribute, export_data_size_bytes);

  return aws::lambda_runtime::invocation_response::success(response.View().WriteCompact(), "application/json");
}

}  // namespace skyrise

int main() {
  const skyrise::WorkerFunction worker;
  worker.HandleRequest();

  return 0;
}
