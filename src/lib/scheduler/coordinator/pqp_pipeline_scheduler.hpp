#pragma once

#include <mutex>

#include <aws/sqs/model/SendMessageRequest.h>

#include "compiler/physical_query_plan/pqp_pipeline.hpp"
#include "lambda_executor.hpp"
#include "operator/abstract_operator.hpp"

namespace skyrise {

enum class PqpPipelineTaskState { kWaiting, kRunning, kFinished };

struct PqpPipelineTask {
  const std::shared_ptr<PqpPipeline> pqp_pipeline;
  size_t fragments_to_finish_count;
  size_t pipeline_predecessors_to_finish_count;
  std::vector<std::shared_ptr<PqpPipelineFragmentExecutionResult>> fragment_execution_results = {};
  PqpPipelineTaskState state = PqpPipelineTaskState::kWaiting;
};

class PqpPipelineScheduler {
 public:
  PqpPipelineScheduler(std::shared_ptr<AbstractPqpPipelineFragmentExecutor> executor,
                       std::vector<std::shared_ptr<PqpPipeline>> pqp_pipelines);

  std::pair<std::shared_ptr<ObjectReference>, std::vector<std::shared_ptr<PqpPipelineTask>>> Execute();

  bool HasError() const;
  std::string GetError() const;

 private:
  bool AllTasksFinished() const;
  void SubmitTasksWithFinishedPredecessors();
  void Submit(const std::shared_ptr<PqpPipelineTask>& task);
  void OnFragmentExecutionFinishedCallback(
      const std::string& pipeline_id,
      const std::shared_ptr<PqpPipelineFragmentExecutionResult>& fragment_execution_result);

  std::shared_ptr<AbstractPqpPipelineFragmentExecutor> executor_;
  const std::vector<std::shared_ptr<PqpPipeline>> pqp_pipelines_;

  std::map<std::string, std::shared_ptr<PqpPipelineTask>> tasks_;

  std::condition_variable_any execution_done_;
  mutable std::mutex mutex_;
  std::string error_message_;
};

}  // namespace skyrise
