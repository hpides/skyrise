#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>

#include "scheduler/worker/abstract_task.hpp"
#include "scheduler/worker/task_executor.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/concurrent/concurrent_queue.hpp"

namespace skyrise {

class FragmentScheduler : public Noncopyable {
 public:
  /**
   * Constructs a new fragment scheduler object with a thread pool size set to the number of
   * available cores.
   */
  FragmentScheduler();

  /**
   * Constructs a new fragment scheduler object with a fixed thread pool size.
   */
  explicit FragmentScheduler(size_t num_threads);

  /**
   * Blocks until all tasks of this scheduler are finished.
   */
  void WaitForAllTasks();

  /**
   * Schedules the given tasks for execution and returns immediately. If no asynchronicity is needed, prefer
   * ScheduleAndWaitForTasks.
   */
  void ScheduleTasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks);

  /**
   * Blocks until all specified tasks are completed. If no asynchronicity is needed, prefer ScheduleAndWaitForTasks.
   */
  void WaitForTasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks);

  void ScheduleAndWaitForTasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks);

  void Schedule(const std::shared_ptr<AbstractTask>& task);

  void OnTaskFinished(const std::shared_ptr<AbstractTask>& task);

 private:
  void Submit(const std::shared_ptr<AbstractTask>& task);

  TaskExecutor executor_;
  std::atomic<size_t> num_scheduled_tasks_ = 0;
  std::mutex lock_;
  std::condition_variable lock_condition_;
};

}  // namespace skyrise
