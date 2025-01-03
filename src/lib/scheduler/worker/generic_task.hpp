/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise).
 */
#pragma once

#include <functional>

#include "abstract_task.hpp"

namespace skyrise {

/**
 * A general purpose Task for any kind of work (i.e., anything that fits into a void()-function) that can be
 * parallelized.
 *
 * Usage example:
 *
 * auto scheduler = std::make_shared<Scheduler>();
 * std::atomic_uint32_t c = 0;
 *
 * auto job0 = std::make_shared<GenericTask>([c]() { c++; });
 * scheduler->Schedule(job0);
 *
 * auto job1 = std::make_shared<GenericTask>([c]() { c++; });
 * scheduler->Schedule(job1);
 *
 * scheduler->WaitForTasks({job0, job1});
 *
 * // c == 2 now
 *
 */
class GenericTask : public AbstractTask {
 public:
  explicit GenericTask(const std::function<void()>& task_function) : task_function_(task_function) {}

 protected:
  void OnExecute() override;

 private:
  std::function<void()> task_function_;
};

}  // namespace skyrise
