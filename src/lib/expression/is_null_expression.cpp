/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "is_null_expression.hpp"

#include <sstream>

#include "utils/assert.hpp"

namespace skyrise {

IsNullExpression::IsNullExpression(const PredicateCondition predicate_condition,
                                   std::shared_ptr<AbstractExpression> operand)
    : AbstractPredicateExpression(predicate_condition, {std::move(operand)}) {
  Assert(predicate_condition_ == PredicateCondition::kIsNull || predicate_condition_ == PredicateCondition::kIsNotNull,
         "IsNullExpression only supports PredicateCondition::kIsNull and PredicateCondition::kIsNotNull.");
}

const std::shared_ptr<AbstractExpression>& IsNullExpression::Operand() const { return arguments_[0]; }

std::shared_ptr<AbstractExpression> IsNullExpression::DeepCopy() const {
  return std::make_shared<IsNullExpression>(predicate_condition_, Operand()->DeepCopy());
}

std::string IsNullExpression::Description(const DescriptionMode mode) const {
  std::stringstream stream;

  if (predicate_condition_ == PredicateCondition::kIsNull) {
    stream << EncloseArgument(*Operand(), mode) << " IS NULL";
  } else {
    stream << EncloseArgument(*Operand(), mode) << " IS NOT NULL";
  }

  return stream.str();
}

ExpressionPrecedence IsNullExpression::Precedence() const { return ExpressionPrecedence::kUnaryPredicate; }

}  // namespace skyrise
