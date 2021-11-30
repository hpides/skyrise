#include "operator_task.hpp"

#include <memory>
#include <unordered_set>
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
    std::unordered_map<std::shared_ptr<AbstractOperator>, std::shared_ptr<OperatorTask>>* task_by_operator,
    const std::shared_ptr<OperatorExecutionContext>& operator_execution_context) {
  auto potential_task = task_by_operator->find(any_operator);
  if (potential_task != task_by_operator->end()) {
    return potential_task->second;
  }

  std::shared_ptr<OperatorTask> task = std::make_shared<OperatorTask>(any_operator, operator_execution_context);
  task_by_operator->emplace(any_operator, task);

  auto add_operator_subtasks = [&](const std::shared_ptr<skyrise::AbstractOperator>& any_operator) {
    if (any_operator) {
      std::shared_ptr<skyrise::OperatorTask> operator_task =
          AddOperatorTasksRecursively(any_operator, task_by_operator, operator_execution_context);
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
  std::unordered_map<std::shared_ptr<AbstractOperator>, std::shared_ptr<OperatorTask>> task_by_operator;
  std::shared_ptr<OperatorTask> root_operator_task =
      AddOperatorTasksRecursively(any_operator, &task_by_operator, operator_execution_context);

  std::vector<std::shared_ptr<AbstractTask>> tasks;
  tasks.reserve(task_by_operator.size());

  for (auto& task_pair : task_by_operator) {
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
