/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "abstract_expression.hpp"
#include "types.hpp"

namespace skyrise {

/**
 * PredicateExpressions are those using a PredicateCondition.
 */
class AbstractPredicateExpression : public AbstractExpression {
 public:
  AbstractPredicateExpression(const PredicateCondition init_predicate_condition,
                              const std::vector<std::shared_ptr<AbstractExpression>>& init_arguments);

  DataType GetDataType() const override;

  const PredicateCondition predicate_condition_;

 protected:
  bool ShallowEquals(const AbstractExpression& expression) const override;
  size_t OnShallowHash() const override;
};

}  // namespace skyrise
