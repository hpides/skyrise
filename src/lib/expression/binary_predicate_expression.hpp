/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "abstract_expression.hpp"
#include "abstract_predicate_expression.hpp"
#include "types.hpp"

namespace skyrise {

class BinaryPredicateExpression : public AbstractPredicateExpression {
 public:
  BinaryPredicateExpression(const PredicateCondition predicate_condition,
                            std::shared_ptr<AbstractExpression> left_operand,
                            std::shared_ptr<AbstractExpression> right_operand);

  const std::shared_ptr<AbstractExpression>& LeftOperand() const;
  const std::shared_ptr<AbstractExpression>& RightOperand() const;

  std::shared_ptr<AbstractExpression> DeepCopy() const override;
  std::string Description(const DescriptionMode mode) const override;

 protected:
  ExpressionPrecedence Precedence() const override;
};

}  // namespace skyrise
