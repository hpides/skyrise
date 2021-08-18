/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "abstract_expression.hpp"
#include "abstract_predicate_expression.hpp"
#include "types.hpp"

namespace skyrise {

class BetweenExpression : public AbstractPredicateExpression {
 public:
  BetweenExpression(const PredicateCondition predicate_condition, std::shared_ptr<AbstractExpression> value,
                    std::shared_ptr<AbstractExpression> lower_bound, std::shared_ptr<AbstractExpression> upper_bound);

  const std::shared_ptr<AbstractExpression>& Value() const;
  const std::shared_ptr<AbstractExpression>& LowerBound() const;
  const std::shared_ptr<AbstractExpression>& UpperBound() const;

  std::shared_ptr<AbstractExpression> DeepCopy() const override;
  std::string Description(const DescriptionMode mode) const override;

 protected:
  ExpressionPrecedence Precedence() const override;
};

}  // namespace skyrise
