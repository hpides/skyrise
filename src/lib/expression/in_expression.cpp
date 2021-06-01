/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "in_expression.hpp"

#include <sstream>

#include "utils/assert.hpp"

namespace skyrise {

InExpression::InExpression(const PredicateCondition init_predicate_condition,
                           const std::shared_ptr<AbstractExpression>& value,
                           const std::shared_ptr<AbstractExpression>& set)
    : AbstractPredicateExpression(init_predicate_condition, {value, set}) {
  DebugAssert(predicate_condition_ == PredicateCondition::kIn || predicate_condition_ == PredicateCondition::kNotIn,
              "Expected either IN or NOT IN as PredicateCondition");
}

std::string InExpression::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << EncloseArgument(*Value(), mode) << " ";
  stream << predicate_condition_ << " ";
  stream << Set()->Description(mode);
  return stream.str();
}

bool InExpression::IsNegated() const { return predicate_condition_ == PredicateCondition::kNotIn; }

const std::shared_ptr<AbstractExpression>& InExpression::Value() const { return arguments_[0]; }

const std::shared_ptr<AbstractExpression>& InExpression::Set() const { return arguments_[1]; }

std::shared_ptr<AbstractExpression> InExpression::OnDeepCopy() const {
  return std::make_shared<InExpression>(predicate_condition_, Value()->DeepCopy(), Set()->DeepCopy());
}

}  // namespace skyrise
