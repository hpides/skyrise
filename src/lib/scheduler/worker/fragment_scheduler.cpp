#include "fragment_scheduler.hpp"

#include "utils/profiling/function_host_information.hpp"

namespace skyrise {

// TODO(tobodner): Move from the current vCPU core count model to a more accurate model based on a cloud function's
// memory capacity (cf.
// sentiatechblog.com/aws-re-invent-2020-day-3-optimizing-lambda-cost-with-multi-threading?utm_source=reddit&utm_medium=social&utm_campaign=day3_lambda).
FragmentScheduler::FragmentScheduler()
    : FragmentScheduler(FunctionHostInformationCollector().CollectInformationResources().cpu_count) {}

FragmentScheduler::FragmentScheduler(size_t num_threads) : executor_(num_threads) {}

void FragmentScheduler::WaitForAllTasks() {
  std::unique_lock<std::mutex> lock(lock_);
  lock_condition_.wait(lock, [&]() { return num_scheduled_tasks_ == 0; });

  // Make sure that all changes to memory are visible to the calling thread.
  std::atomic_thread_fence(std::memory_order_seq_cst);
}

void FragmentScheduler::WaitForTasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks) {
  if (executor_.IsExecutorThread()) {
    const auto is_finished = [&tasks]() -> bool {
      return std::ranges::all_of(tasks, [](const std::shared_ptr<AbstractTask>& task) { return task->IsDone(); });
    };

    while (!is_finished()) {
      executor_.WorkerProcessSingleItemNoWait();
    }
  } else {
    for (const auto& task : tasks) {
      task->Join();
    }
  }
}
void FragmentScheduler::Schedule(const std::shared_ptr<AbstractTask>& task) {
  if (!task->TryTransitionToScheduled()) {
    return;
  }

  ++num_scheduled_tasks_;

  if (task->IsReady()) {
    Submit(task);
  } else {
    // If a task is not yet ready, its predecessors must be executed first.
    for (const std::shared_ptr<skyrise::AbstractTask>& predecessor_task : task->Predecessors()) {
      Schedule(predecessor_task);
    }
  }
}

void FragmentScheduler::Submit(const std::shared_ptr<AbstractTask>& task) {
  DebugAssert(task->IsScheduled(), "Task which are submitted need to be scheduled first.");
  DebugAssert(task->IsReady(), "Task which are submitted need to be ready first.");

  if (!task->TryTransitionToEnqueued()) {
    return;
  }

  executor_.Submit([this, task]() {
    task->Execute();
    OnTaskFinished(task);
  });
}

void FragmentScheduler::OnTaskFinished(const std::shared_ptr<AbstractTask>& task) {
  for (const auto& successor : task->Successors()) {
    if (successor->AtomicDecrementPredecessorCount() == 0 && successor->IsScheduled()) {
      Submit(successor);
    }
  }

  const size_t num_tasks_still_scheduled = --num_scheduled_tasks_;

  if (num_tasks_still_scheduled == 0) {
    lock_condition_.notify_all();
  }
}

void FragmentScheduler::ScheduleTasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks) {
  for (const auto& task : tasks) {
    Schedule(task);
  }
}

void FragmentScheduler::ScheduleAndWaitForTasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks) {
  ScheduleTasks(tasks);
  WaitForTasks(tasks);
}

}  // namespace skyrise
