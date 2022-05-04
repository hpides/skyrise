/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "expression/abstract_expression.hpp"
#include "filter/abstract_filter_implementation.hpp"
#include "operator/abstract_operator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace skyrise {

class FilterOperator : public AbstractOperator {
 public:
  FilterOperator(std::shared_ptr<const AbstractOperator> input_operator, std::shared_ptr<AbstractExpression> predicate);

  const std::shared_ptr<AbstractExpression>& Predicate() const;

  const std::string& Name() const override;
  std::string Description(DescriptionMode description_mode) const override;

  /**
   * Create a suiting filter implementation based on the predicate type.
   * Currently, there is only one general purpose filter implementation available, which is based on the
   * ExpressionEvaluator. Predicate-specific, efficient filter implementations are planned to be added in the future.
   */
  std::unique_ptr<AbstractFilterImplementation> CreateImplementation();

 protected:
  std::shared_ptr<const Table> OnExecute(
      const std::shared_ptr<OperatorExecutionContext>& operator_execution_context) override;

  void OnCleanup() override;

 private:
  const std::shared_ptr<AbstractExpression> predicate_;

  std::unique_ptr<AbstractFilterImplementation> implementation_;

  /**
   * The description of the implementation so that it is available after resetting the implementation_ in OnCleanup().
   */
  std::string implementation_description_ = "Unset";
};

}  // namespace skyrise
