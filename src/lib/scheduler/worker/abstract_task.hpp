/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise).
 */
#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "types.hpp"

namespace skyrise {

/**
 * @brief Overview of the different task states and involved transitions:
 *
 *    +-----------+
 *    |  Created  |
 *    +-----------+
 *          | Schedule()
 *          v
 *    +-----------+
 *    | Scheduled |
 *    +-----------+
 *          | Submit()
 *          v
 *    +-----------+
 *    | Enqueued |
 *    +-----------+
 *          | Execute()
 *          v
 *    +-----------+
 *    |  Started  |
 *    +-----------+
 *          | Execute() finishes
 *          v
 *    +-----------+
 *    |   Done    |
 *    +-----------+
 *
 *  1. All tasks are initialized in TaskState::kCreated.
 *  2. A task changes to TaskState::kScheduled once the task is scheduled by a scheduler.
 *  3. Once the scheduler submits a task to the executor queue, the task changes its state to TaskState::kEnqueued.
 *  4. Once the task is taken by an executor thread, it transitions to TaskState:kStarted.
 *  5. After finishing its work, Execute() transitions the task to TaskState::kDone.
 */

// The state enum values are declared in progressive order to allow for comparisons involving the >, >= operators.
enum class TaskState { kCreated, kScheduled, kEnqueued, kStarted, kDone };
static_assert(static_cast<std::underlying_type_t<TaskState>>(TaskState::kCreated) == 0,
              "TaskState::kCreated is not equal to 0. TaskState enum values are expected to be ordered.");

/**
 * Base class for anything that can be scheduled by an scheduler and executed by a executor.
 *
 * Derive and implement logic in OnExecute()
 */
class AbstractTask : public std::enable_shared_from_this<AbstractTask> {
 public:
  virtual ~AbstractTask() = default;

  /**
   * Unique ID of a task. Currently not in use, but really helpful for debugging.
   */
  TaskId Id() const;

  /**
   * @return All dependencies are done.
   */
  bool IsReady() const;

  /**
   * @return true when the task is scheduled or was scheduled successfully.
   */
  bool IsScheduled() const;

  /**
   * @return The task finished executing.
   */
  bool IsDone() const;

  /**
   * Description for debugging purposes.
   */
  virtual std::string Description() const;

  /**
   * Task ids are determined on scheduling, no one else but the Scheduler should have any reason to call this
   * @param id id, unique during the lifetime of the program, of the task.
   */
  void SetId(TaskId id);

  /**
   * Make this Task the dependency of another @param successor Task that will be executed after this.
   */
  void SetAsPredecessorOf(const std::shared_ptr<AbstractTask>& successor);

  bool TryTransitionToEnqueued();

  bool TryTransitionToScheduled();

  /**
   * @return the predecessors of this Task.
   */
  const std::vector<std::shared_ptr<AbstractTask>>& Predecessors() const;

  /**
   * @return the successors of this Task.
   */
  const std::vector<std::shared_ptr<AbstractTask>>& Successors() const;

  /**
   * Callback to be executed right after the Task finished.
   * Notice the execution of the callback might happen on ANY thread.
   */
  void SetDoneCallback(std::function<void()> done_callback);

  /**
   * Executes the task in the current Thread, blocks until all operations are finished.
   */
  void Execute();

  /**
   * Called by a dependency when it finished execution.
   */
  size_t AtomicDecrementPredecessorCount();

  /**
   * Blocks the calling thread until the Task finished executing.
   * This will block the calling thread.
   */
  void Join();

  TaskState State() const;

 protected:
  virtual void OnExecute() = 0;

  /**
   * Transitions the task's state to @param new_state.
   * @return true on success and
   *          false if another caller/thread/worker was faster in progressing this task's state.
   */
  [[nodiscard]] bool TryTransitionTo(TaskState new_state);

 private:
  std::atomic<TaskId> id_ = kInvalidTaskId;
  std::function<void()> done_callback_;

  // For dependencies.
  std::atomic_uint32_t pending_predecessors_ = 0;
  std::vector<std::shared_ptr<AbstractTask>> predecessors_;
  std::vector<std::shared_ptr<AbstractTask>> successors_;

  // For state management.
  std::atomic<TaskState> state_ = TaskState::kCreated;
  std::mutex transition_to_mutex_;

  // For making Tasks join()-able.
  std::condition_variable done_condition_variable_;
  std::mutex done_condition_variable_mutex_;

  // Purely for debugging purposes, in order to be able to identify tasks after they have been scheduled.
  std::string description_;
};

}  // namespace skyrise
