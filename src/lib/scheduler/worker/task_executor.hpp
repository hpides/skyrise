#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <set>
#include <thread>
#include <vector>

#include "scheduler/worker/abstract_task.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/concurrent/concurrent_queue.hpp"

namespace skyrise {

// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions,hicpp-special-member-functions)
class TaskExecutor : public Noncopyable {
 public:
  explicit TaskExecutor(size_t num_threads);
  ~TaskExecutor();

  void Submit(std::function<void(void)> job);
  void WorkerProcessSingleItemNoWait();
  bool IsExecutorThread();

 private:
  void WorkerMain();
  void WorkerProcessSingleItem();

  std::atomic<bool> is_running_ = true;
  std::set<std::thread::id> thread_ids_;
  ConcurrentQueue<std::function<void()>> queue_;
  std::vector<std::thread> thread_pool_;
};

}  // namespace skyrise
