#include "coordinator_function.hpp"

#include <vector>

#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/sqs/model/DeleteQueueRequest.h>

#include "aws/dynamodb/model/ScanRequest.h"
#include "client/coordinator_client.hpp"
#include "compiler/abstract_compiler.hpp"
#include "compiler/physical_query_plan/pqp_pipeline.hpp"
#include "constants.hpp"
#include "scheduler/coordinator/lambda_executor.hpp"
#include "scheduler/coordinator/pqp_pipeline_scheduler.hpp"

namespace skyrise {

struct WorkerStatistics final {
  explicit WorkerStatistics() = default;

  size_t worker_memory_size_mb{0};
  size_t worker_invocation_count{0};
  size_t worker_accumulated_runtime_ms{0};
  std::map<std::string, size_t> worker_accumulated_request_counts;
};

WorkerStatistics GetWorkerStatistics(const std::vector<std::shared_ptr<PqpPipelineTask>>& pipeline_tasks) {
  WorkerStatistics statistics;

  for (const auto& pipeline_task : pipeline_tasks) {
    const std::vector<std::shared_ptr<PqpPipelineFragmentExecutionResult>>& fragment_execution_results =
        pipeline_task->fragment_execution_results;
    if (fragment_execution_results.empty()) {
      continue;
    }

    if (statistics.worker_memory_size_mb == 0) {
      statistics.worker_memory_size_mb = fragment_execution_results.front()->function_instance_size_mb;
    }

    statistics.worker_invocation_count += fragment_execution_results.size();

    for (const auto& fragment_execution_result : fragment_execution_results) {
      statistics.worker_accumulated_runtime_ms += fragment_execution_result->runtime_ms;

      const auto& metering = fragment_execution_result->metering.View();
      for (const auto& request_type : metering.GetAllObjects()) {
        statistics.worker_accumulated_request_counts[request_type.first] += request_type.second.GetInteger("finished");
      }
    }
  }

  return statistics;
}

bool CoordinatorFunction::ValidateRequest(const Aws::Utils::Json::JsonView& request) {
  return request.KeyExists(kCoordinatorRequestCompilerNameAttribute) &&
         (request.KeyExists(kCoordinatorRequestQueryPlanAttribute) ||
          request.KeyExists(kCoordinatorRequestQueryStringAttribute)) &&
         request.KeyExists(kCoordinatorRequestScaleFactorAttribute) &&
         request.KeyExists(kCoordinatorRequestStoragePrefixAttribute) &&
         request.KeyExists(kCoordinatorRequestWorkerFunctionAttribute);
}

aws::lambda_runtime::invocation_response CoordinatorFunction::OnHandleRequest(
    const Aws::Utils::Json::JsonView& request) const {
  AWS_LOGSTREAM_DEBUG(kCoordinatorTag.c_str(), std::string("Coordinator request: ") + request.WriteCompact());
  if (!ValidateRequest(request)) {
    return aws::lambda_runtime::invocation_response::failure("Invalid parameters provided.", "text/plain");
  }

  Aws::Utils::Json::JsonValue response;

  const auto compiler_config = AbstractCompilerConfig::FromJson(request);
  const auto compiler = compiler_config->GenerateCompiler();
  const std::vector<std::shared_ptr<PqpPipeline>> pipelines = compiler->GeneratePqp();
  const std::string worker_function_name = request.GetString(kCoordinatorRequestWorkerFunctionAttribute);

  const auto client = std::make_shared<CoordinatorClient>();
  const auto executor = std::make_shared<LambdaExecutor>(client, worker_function_name);

  try {
    PqpPipelineScheduler scheduler(executor, pipelines);
    AWS_LOGSTREAM_INFO(kCoordinatorTag.c_str(), "Starting execution.");
    const auto execution_start = std::chrono::steady_clock::now();
    const auto [result, tasks] = scheduler.Execute();
    if (scheduler.HasError()) {
      return aws::lambda_runtime::invocation_response::failure(scheduler.GetError(), "text/plain");
    }
    AWS_LOGSTREAM_INFO(kCoordinatorTag.c_str(), "Finished execution.");
    const size_t execution_runtime_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - execution_start)
            .count();

    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    const char* coordinator_memory_size_mb_string = std::getenv("AWS_LAMBDA_FUNCTION_MEMORY_SIZE");
    const size_t coordinator_memory_size_mb =
        coordinator_memory_size_mb_string != nullptr ? std::stoull(coordinator_memory_size_mb_string, nullptr, 10) : 0;

    WorkerStatistics worker_statistics = GetWorkerStatistics(tasks);
    Aws::Utils::Json::JsonValue serialized_statistics;
    serialized_statistics.WithInteger(kCoordinatorResponseExecutionRuntimeAttribute, execution_runtime_ms)
        .WithInteger(kCoordinatorResponseMemorySizeAttribute, coordinator_memory_size_mb)
        .WithInteger(kCoordinatorResponseWorkerMemorySizeAttribute, worker_statistics.worker_memory_size_mb)
        .WithInteger(kCoordinatorResponseWorkerInvocationCountAttribute, worker_statistics.worker_invocation_count)
        .WithInteger(kCoordinatorResponseWorkerAccumulatedRuntimeAttribute,
                     worker_statistics.worker_accumulated_runtime_ms);
    // .WithInt64("sqs_requests", request_counter["SQS:SendMessage"]);
    // .WithInt64("put_requests", request_counter["S3:PutObject"]);
    // .WithInt64("get_requests", request_counter["S3:GetObject"]);

    response.WithObject(kCoordinatorResponseResultObjectAttribute, result->ToJson())
        .WithObject(kCoordinatorResponseExecutionStatisticsAttribute, serialized_statistics);

  } catch (const std::exception& exception) {
    AWS_LOGSTREAM_INFO(kCoordinatorTag.c_str(), exception.what());
  }

  return aws::lambda_runtime::invocation_response::success(response.View().WriteCompact(), "application/json");
}

}  // namespace skyrise

int main() {
  const skyrise::CoordinatorFunction coordinator;
  coordinator.HandleRequest();

  return 0;
}
