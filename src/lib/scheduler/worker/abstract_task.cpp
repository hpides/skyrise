#include "abstract_task.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "types.hpp"
#include "utils/assert.hpp"

namespace skyrise {

TaskId AbstractTask::Id() const { return id_; }

bool AbstractTask::IsReady() const { return pending_predecessors_ == 0; }

bool AbstractTask::IsScheduled() const { return state_ >= TaskState::kScheduled; }

bool AbstractTask::IsDone() const { return state_ == TaskState::kDone; }

std::string AbstractTask::Description() const {
  return description_.empty() ? "{Task with id: " + std::to_string(id_) + "}" : description_;
}

void AbstractTask::SetId(TaskId id) { id_ = id; }

void AbstractTask::SetAsPredecessorOf(const std::shared_ptr<AbstractTask>& successor) {
  if (std::ranges::find(successors_, successor) != successors_.cend()) {
    return;
  }

  successors_.push_back(successor);
  successor->predecessors_.emplace_back(shared_from_this());

  const std::lock_guard<std::mutex> lock(done_condition_variable_mutex_);
  if (!IsDone()) {
    ++successor->pending_predecessors_;
  }
}

const std::vector<std::shared_ptr<AbstractTask>>& AbstractTask::Predecessors() const { return predecessors_; }

const std::vector<std::shared_ptr<AbstractTask>>& AbstractTask::Successors() const { return successors_; }

void AbstractTask::SetDoneCallback(std::function<void()> done_callback) {
  DebugAssert(!IsScheduled(), "Possible race: Do not set callback after the Task was scheduled.");

  done_callback_ = std::move(done_callback);
}

void AbstractTask::Join() {
  std::unique_lock<std::mutex> lock(done_condition_variable_mutex_);

  if (IsDone()) {
    return;
  }

  DebugAssert(IsScheduled(), "Task must be scheduled before it can be waited for.");
  done_condition_variable_.wait(lock, [&]() { return IsDone(); });
}

void AbstractTask::Execute() {
  const bool success_started = TryTransitionTo(TaskState::kStarted);
  Assert(success_started, "Expected successful transition to TaskState::Started.");

  DebugAssert(IsReady(), "Task must not be executed before its dependencies are done.");

  // We need to make sure that data written by the scheduling thread is visible in the thread executing the task.
  std::atomic_thread_fence(std::memory_order_seq_cst);

  OnExecute();

  std::unique_lock<std::mutex> lock(done_condition_variable_mutex_);
  const bool success_done = TryTransitionTo(TaskState::kDone);
  Assert(success_done, "Expected successful transition to TaskState::Done.");
  lock.unlock();

  if (done_callback_) {
    done_callback_();
  }

  done_condition_variable_.notify_all();
}

TaskState AbstractTask::State() const { return state_; }

size_t AbstractTask::AtomicDecrementPredecessorCount() {
  Assert(pending_predecessors_ > 0, "The count of pending predecessors equals zero and cannot be decremented.");
  const size_t new_predecessor_count = --pending_predecessors_;  // atomically decrement
  return new_predecessor_count;
}

bool AbstractTask::TryTransitionTo(TaskState new_state) {
  const std::lock_guard<std::mutex> lock(transition_to_mutex_);
  switch (new_state) {
    case TaskState::kScheduled:
      if (state_ >= TaskState::kScheduled) {
        return false;
      }
      Assert(state_ == TaskState::kCreated, "Illegal state transition to TaskState::kScheduled.");
      break;
    case TaskState::kEnqueued:
      if (state_ >= TaskState::kEnqueued) {
        return false;
      }
      Assert(state_ == TaskState::kScheduled, "Illegal state transition to TaskState::kEnqueued.");
      break;
    case TaskState::kStarted:
      Assert(state_ == TaskState::kEnqueued,
             "Illegal state transition to TaskState::kStarted: Task should have been enqueued before being executed.");
      break;
    case TaskState::kDone:
      Assert(state_ == TaskState::kStarted, "Illegal state transition to TaskState::kDone.");
      break;
    default:
      Fail("Unexpected target state in AbstractTask.");
  }

  state_.exchange(new_state);
  return true;
}
/**
 * Try to change the state of the task to TaskState::kScheduled.
 * @return false if the task is already scheduled, true otherwise.
 */
bool AbstractTask::TryTransitionToScheduled() { return TryTransitionTo(TaskState::kScheduled); }

/**
 * Try to change the state of the task to TaskState::kEnqueued.
 * @return false if the task is already submitted, true otherwise.
 */
bool AbstractTask::TryTransitionToEnqueued() { return TryTransitionTo(TaskState::kEnqueued); }

}  // namespace skyrise
