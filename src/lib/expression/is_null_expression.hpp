/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "abstract_expression.hpp"
#include "abstract_predicate_expression.hpp"
#include "types.hpp"

namespace skyrise {

class IsNullExpression : public AbstractPredicateExpression {
 public:
  IsNullExpression(const PredicateCondition predicate_condition, std::shared_ptr<AbstractExpression> operand);

  const std::shared_ptr<AbstractExpression>& Operand() const;

  std::shared_ptr<AbstractExpression> DeepCopy() const override;
  std::string Description(const DescriptionMode mode) const override;

 protected:
  ExpressionPrecedence Precedence() const override;
};

}  // namespace skyrise
