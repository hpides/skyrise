/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "in_expression.hpp"

#include <sstream>

#include "utils/assert.hpp"

namespace skyrise {

InExpression::InExpression(const PredicateCondition predicate_condition, std::shared_ptr<AbstractExpression> value,
                           std::shared_ptr<AbstractExpression> set)
    : AbstractPredicateExpression(predicate_condition, {std::move(value), std::move(set)}) {
  DebugAssert(predicate_condition_ == PredicateCondition::kIn || predicate_condition_ == PredicateCondition::kNotIn,
              "Expected either IN or NOT IN as PredicateCondition.");
}

bool InExpression::IsNegated() const { return predicate_condition_ == PredicateCondition::kNotIn; }

const std::shared_ptr<AbstractExpression>& InExpression::Value() const { return arguments_[0]; }

const std::shared_ptr<AbstractExpression>& InExpression::Set() const { return arguments_[1]; }

std::shared_ptr<AbstractExpression> InExpression::DeepCopy() const {
  return std::make_shared<InExpression>(predicate_condition_, Value()->DeepCopy(), Set()->DeepCopy());
}

std::string InExpression::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << EncloseArgument(*Value(), mode) << " ";
  stream << predicate_condition_ << " ";
  stream << Set()->Description(mode);
  return stream.str();
}

}  // namespace skyrise
