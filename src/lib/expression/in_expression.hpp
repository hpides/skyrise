/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "abstract_predicate_expression.hpp"

namespace skyrise {

/**
 * SQL's IN.
 */
class InExpression : public AbstractPredicateExpression {
 public:
  InExpression(const PredicateCondition predicate_condition, std::shared_ptr<AbstractExpression> value,
               std::shared_ptr<AbstractExpression> set);

  std::string Description(const DescriptionMode mode) const override;

  /**
   * Shorthand checking for PredicateCondition::kNotIn.
   *
   * @return The result of predicate_condition == PredicateCondition::kNotIn.
   */
  bool IsNegated() const;

  const std::shared_ptr<AbstractExpression>& Value() const;
  const std::shared_ptr<AbstractExpression>& Set() const;

  std::shared_ptr<AbstractExpression> DeepCopy() const override;
};

}  // namespace skyrise
