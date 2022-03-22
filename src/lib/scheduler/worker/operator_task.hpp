/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise).
 */
#pragma once

#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "operator/abstract_operator.hpp"
#include "operator/execution_context.hpp"
#include "scheduler/worker/abstract_task.hpp"

namespace skyrise {

class OperatorTask : public AbstractTask {
 public:
  // We do not like abbreviations, but "operator" is a keyword.
  OperatorTask(std::shared_ptr<AbstractOperator> any_operator,
               std::shared_ptr<OperatorExecutionContext> operator_execution_context);

  /**
   * Creates tasks recursively from the given operator @param any_operator and sets task dependencies automatically.
   * @return a pair, consisting of a vector of unordered tasks and a pointer to the root operator task that would
   *          otherwise be hidden inside the vector.
   */
  static std::pair<std::vector<std::shared_ptr<AbstractTask>>, std::shared_ptr<OperatorTask>> GenerateTasksFromOperator(
      const std::shared_ptr<AbstractOperator>& any_operator,
      const std::shared_ptr<OperatorExecutionContext>& operator_execution_context);

  const std::shared_ptr<AbstractOperator>& GetOperator() const;

  std::string Description() const override;

 protected:
  void OnExecute() override;

 private:
  static std::shared_ptr<OperatorTask> AddOperatorTasksRecursively(
      const std::shared_ptr<AbstractOperator>& any_operator,
      std::unordered_map<std::shared_ptr<AbstractOperator>, std::shared_ptr<OperatorTask>>* operator_to_task,
      const std::shared_ptr<OperatorExecutionContext>& operator_execution_context);

  std::shared_ptr<AbstractOperator> any_operator_;
  const std::shared_ptr<OperatorExecutionContext> operator_execution_context_;
};

}  // namespace skyrise
