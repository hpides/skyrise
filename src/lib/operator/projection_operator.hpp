/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "abstract_operator.hpp"
#include "expression/abstract_expression.hpp"

namespace skyrise {

class ProjectionOperator : public AbstractOperator {
 public:
  ProjectionOperator(std::shared_ptr<const AbstractOperator> input_operator,
                     const std::vector<std::shared_ptr<AbstractExpression>>& expressions);
  const std::string& Name() const override;

 protected:
  std::shared_ptr<const Table> OnExecute(const std::shared_ptr<OperatorExecutionContext>& context) override;

  const std::vector<std::shared_ptr<AbstractExpression>> expressions_;
};

}  // namespace skyrise
