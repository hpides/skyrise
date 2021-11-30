#include "task_executor.hpp"

namespace skyrise {

TaskExecutor::TaskExecutor(size_t num_threads) {
  thread_pool_.reserve(num_threads);
  for (size_t i = 0; i < num_threads; ++i) {
    thread_pool_.emplace_back([&]() { WorkerMain(); });
    thread_ids_.insert(thread_pool_.back().get_id());
  }
}

TaskExecutor::~TaskExecutor() {
  is_running_ = false;
  queue_.Close();
  for (auto& thread : thread_pool_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

bool TaskExecutor::IsExecutorThread() { return thread_ids_.find(std::this_thread::get_id()) != thread_ids_.end(); }

void TaskExecutor::WorkerMain() {
  while (is_running_) {
    WorkerProcessSingleItem();
  }
}

void TaskExecutor::WorkerProcessSingleItemNoWait() {
  DebugAssert(thread_ids_.find(std::this_thread::get_id()) != thread_ids_.end(),
              "This function should only be called from an executor thread.");

  std::function<void()> job;
  const bool has_job = queue_.TryPop(&job);
  if (has_job && is_running_) {
    job();
  } else {
    std::this_thread::yield();
  }
}

void TaskExecutor::WorkerProcessSingleItem() {
  std::function<void()> job;
  const bool has_job = queue_.Pop(&job);
  if (has_job) {
    job();
  }
}

void TaskExecutor::Submit(std::function<void(void)> job) { queue_.Push(std::move(job)); }

}  // namespace skyrise
