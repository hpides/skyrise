#include "operator_task.hpp"

#include <memory>
#include <utility>

#include "operator/abstract_operator.hpp"
#include "scheduler/worker/generic_task.hpp"

namespace skyrise {

OperatorTask::OperatorTask(std::shared_ptr<AbstractOperator> any_operator,
                           std::shared_ptr<OperatorExecutionContext> operator_execution_context)
    : any_operator_(std::move(any_operator)), operator_execution_context_(std::move(operator_execution_context)) {}

std::string OperatorTask::Description() const {
  return "OperatorTask with id: " + std::to_string(Id()) + " for operator: " + any_operator_->Description();
}

std::shared_ptr<OperatorTask> OperatorTask::AddOperatorTasksRecursively(
    const std::shared_ptr<AbstractOperator>& any_operator,
    std::unordered_map<std::shared_ptr<AbstractOperator>, std::shared_ptr<OperatorTask>>* operator_to_task,
    const std::shared_ptr<OperatorExecutionContext>& operator_execution_context) {
  auto potential_task = operator_to_task->find(any_operator);
  if (potential_task != operator_to_task->end()) {
    return potential_task->second;
  }

  std::shared_ptr<OperatorTask> task = std::make_shared<OperatorTask>(any_operator, operator_execution_context);
  operator_to_task->emplace(any_operator, task);

  auto add_operator_subtasks = [&](const std::shared_ptr<skyrise::AbstractOperator>& any_operator) {
    if (any_operator) {
      const std::shared_ptr<skyrise::OperatorTask> operator_task =
          AddOperatorTasksRecursively(any_operator, operator_to_task, operator_execution_context);
      if (operator_task) {
        operator_task->SetAsPredecessorOf(task);
      }
    }
  };

  add_operator_subtasks(any_operator->MutableLeftInput());
  add_operator_subtasks(any_operator->MutableRightInput());

  return task;
}

std::pair<std::vector<std::shared_ptr<AbstractTask>>, std::shared_ptr<OperatorTask>>
OperatorTask::GenerateTasksFromOperator(const std::shared_ptr<AbstractOperator>& any_operator,
                                        const std::shared_ptr<OperatorExecutionContext>& operator_execution_context) {
  std::unordered_map<std::shared_ptr<AbstractOperator>, std::shared_ptr<OperatorTask>> operator_to_task;
  std::shared_ptr<OperatorTask> root_operator_task =
      AddOperatorTasksRecursively(any_operator, &operator_to_task, operator_execution_context);

  std::vector<std::shared_ptr<AbstractTask>> tasks;
  tasks.reserve(operator_to_task.size());

  for (auto& task_pair : operator_to_task) {
    tasks.push_back(std::move(task_pair.second));
  }

  return std::make_pair(std::move(tasks), std::move(root_operator_task));
}

const std::shared_ptr<AbstractOperator>& OperatorTask::GetOperator() const { return any_operator_; }

void OperatorTask::OnExecute() {
  any_operator_->Execute(operator_execution_context_);
  for (const std::shared_ptr<skyrise::AbstractTask>& weak_predecessor : Predecessors()) {
    const std::shared_ptr<skyrise::OperatorTask> predecessor =
        std::dynamic_pointer_cast<OperatorTask>(weak_predecessor);

    Assert(predecessor != nullptr, "Predecessor of OperatorTask is not an OperatorTask itself.");
    bool previous_operator_still_needed = false;
    for (const std::shared_ptr<skyrise::AbstractTask>& successor : predecessor->Successors()) {
      if (successor.get() != this && !successor->IsDone()) {
        previous_operator_still_needed = true;
        break;
      }
    }

    if (!previous_operator_still_needed) {
      predecessor->GetOperator()->ClearOutput();
    }
  }
}

}  // namespace skyrise
