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
  IsNullExpression(const PredicateCondition init_predicate_condition,
                   const std::shared_ptr<AbstractExpression>& operand);

  const std::shared_ptr<AbstractExpression>& Operand() const;

  std::string Description(const DescriptionMode mode) const override;

 protected:
  ExpressionPrecedence Precedence() const override;
  std::shared_ptr<AbstractExpression> OnDeepCopy() const override;
};

}  // namespace skyrise
