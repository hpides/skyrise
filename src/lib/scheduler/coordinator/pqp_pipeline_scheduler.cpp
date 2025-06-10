#include "scheduler/coordinator/pqp_pipeline_scheduler.hpp"

#include <boost/container_hash/hash.hpp>

#include "configuration.hpp"
#include "utils/time.hpp"

namespace skyrise {

PqpPipelineScheduler::PqpPipelineScheduler(std::shared_ptr<AbstractPqpPipelineFragmentExecutor> executor,
                                           std::vector<std::shared_ptr<PqpPipeline>> pqp_pipelines)
    : executor_(std::move(executor)), pqp_pipelines_(std::move(pqp_pipelines)) {
  for (const auto& pipeline : pqp_pipelines_) {
    Assert(!pipeline->FragmentDefinitions().empty(), "A pipeline needs at least one fragment definition.");
    tasks_.emplace(pipeline->Identity(), std::make_shared<PqpPipelineTask>(PqpPipelineTask{
                                             .pqp_pipeline = pipeline,
                                             .fragments_to_finish_count = pipeline->FragmentDefinitions().size(),
                                             .pipeline_predecessors_to_finish_count = pipeline->Predecessors().size(),
                                             .fragment_execution_results = {}}));
  }
};

std::pair<std::shared_ptr<ObjectReference>, std::vector<std::shared_ptr<PqpPipelineTask>>>
PqpPipelineScheduler::Execute() {
  std::unique_lock lock(mutex_);

  SubmitTasksWithFinishedPredecessors();

  execution_done_.wait(lock, [this]() { return AllTasksFinished(); });

  std::shared_ptr<ObjectReference> final_result;
  std::vector<std::shared_ptr<PqpPipelineTask>> tasks;
  tasks.reserve(tasks_.size());
  for (const auto& [id, task] : tasks_) {
    tasks.push_back(task);
    if (task->pqp_pipeline->Successors().empty()) {  // Final pipeline
      final_result = std::make_shared<ObjectReference>(task->pqp_pipeline->FragmentDefinitions()[0].target_object);
    }
  }

  return {final_result, tasks};
}

bool PqpPipelineScheduler::HasError() const { return !error_message_.empty(); }

std::string PqpPipelineScheduler::GetError() const { return error_message_; }

void PqpPipelineScheduler::SubmitTasksWithFinishedPredecessors() {
  for (const auto& [id, task] : tasks_) {
    if (task->pipeline_predecessors_to_finish_count == 0 && task->state == PqpPipelineTaskState::kWaiting) {
      Submit(task);
    }
  }
}

void PqpPipelineScheduler::Submit(const std::shared_ptr<PqpPipelineTask>& task) {
  DebugAssert(task->state == PqpPipelineTaskState::kWaiting, "Invalid task state for submission.");
  task->state = PqpPipelineTaskState::kRunning;
  executor_->Execute(task->pqp_pipeline,
                     [pipeline_id = task->pqp_pipeline->Identity(),
                      this](const std::shared_ptr<PqpPipelineFragmentExecutionResult>& fragment_execution_result) {
                       OnFragmentExecutionFinishedCallback(pipeline_id, fragment_execution_result);
                     });
}

bool PqpPipelineScheduler::AllTasksFinished() const {
  return std::ranges::all_of(tasks_,
                             [](const auto& task) { return task.second->state == PqpPipelineTaskState::kFinished; });
}

void PqpPipelineScheduler::OnFragmentExecutionFinishedCallback(
    const std::string& pipeline_id,
    const std::shared_ptr<PqpPipelineFragmentExecutionResult>& fragment_execution_result) {
  if (!fragment_execution_result->is_success) {
    std::lock_guard lock(mutex_);
    std::stringstream error;
    error << "Worker with id: " << fragment_execution_result->worker_id << " in pipeline "
          << fragment_execution_result->pipeline_id << " has thrown an error: " << fragment_execution_result->message
          << "\n";
    error_message_ = error.str();
    execution_done_.notify_all();
  }

  if (tasks_.empty()) {
    Assert(HasError(), "Invalid state: Scheduler ran out of tasks, but no error was set.");
    return;
  }

  std::lock_guard lock(mutex_);
  auto& task = tasks_.at(pipeline_id);
  task->fragment_execution_results.emplace_back(fragment_execution_result);
  task->fragments_to_finish_count--;

  if (task->fragments_to_finish_count == 0) {
    task->state = PqpPipelineTaskState::kFinished;

    for (const auto& successor : task->pqp_pipeline->Successors()) {
      tasks_.at(successor->Identity())->pipeline_predecessors_to_finish_count--;
    }

    SubmitTasksWithFinishedPredecessors();
  }

  if (AllTasksFinished()) {
    execution_done_.notify_all();
  }
};

}  // namespace skyrise
