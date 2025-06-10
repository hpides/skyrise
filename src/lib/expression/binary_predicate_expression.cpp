/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "binary_predicate_expression.hpp"

#include <sstream>

#include "types.hpp"
#include "utils/assert.hpp"

namespace skyrise {

BinaryPredicateExpression::BinaryPredicateExpression(const PredicateCondition predicate_condition,
                                                     std::shared_ptr<AbstractExpression> left_operand,
                                                     std::shared_ptr<AbstractExpression> right_operand)
    : AbstractPredicateExpression(predicate_condition, {std::move(left_operand), std::move(right_operand)}) {
  if constexpr (SKYRISE_DEBUG) {
    const std::vector<PredicateCondition> valid_predicate_conditions = {
        PredicateCondition::kEquals,      PredicateCondition::kNotEquals,
        PredicateCondition::kGreaterThan, PredicateCondition::kGreaterThanEquals,
        PredicateCondition::kLessThan,    PredicateCondition::kLessThanEquals,
        PredicateCondition::kLike,        PredicateCondition::kNotLike};
    const auto iterator = std::ranges::find(valid_predicate_conditions, predicate_condition_);
    Assert(iterator != valid_predicate_conditions.cend(),
           "Specified PredicateCondition is not valid for a BinaryPredicateExpression.");
  }
}

const std::shared_ptr<AbstractExpression>& BinaryPredicateExpression::LeftOperand() const { return arguments_[0]; }

const std::shared_ptr<AbstractExpression>& BinaryPredicateExpression::RightOperand() const { return arguments_[1]; }

std::shared_ptr<AbstractExpression> BinaryPredicateExpression::DeepCopy() const {
  return std::make_shared<BinaryPredicateExpression>(predicate_condition_, LeftOperand()->DeepCopy(),
                                                     RightOperand()->DeepCopy());
}

std::string BinaryPredicateExpression::Description(const DescriptionMode mode) const {
  std::stringstream stream;

  stream << EncloseArgument(*LeftOperand(), mode) << " ";
  stream << predicate_condition_ << " ";
  stream << EncloseArgument(*RightOperand(), mode);

  return stream.str();
}

ExpressionPrecedence BinaryPredicateExpression::Precedence() const {
  return ExpressionPrecedence::kBinaryTernaryPredicate;
}

}  // namespace skyrise
