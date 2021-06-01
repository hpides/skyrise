/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "between_expression.hpp"

#include <sstream>

#include "utils/assert.hpp"

namespace skyrise {

BetweenExpression::BetweenExpression(const PredicateCondition init_predicate_condition,
                                     const std::shared_ptr<AbstractExpression>& value,
                                     const std::shared_ptr<AbstractExpression>& lower_bound,
                                     const std::shared_ptr<AbstractExpression>& upper_bound)
    : AbstractPredicateExpression(init_predicate_condition, {value, lower_bound, upper_bound}) {
  Assert(IsBetweenPredicateCondition(predicate_condition_), "Unsupported PredicateCondition.");
}

const std::shared_ptr<AbstractExpression>& BetweenExpression::Value() const { return arguments_[0]; }

const std::shared_ptr<AbstractExpression>& BetweenExpression::LowerBound() const { return arguments_[1]; }

const std::shared_ptr<AbstractExpression>& BetweenExpression::UpperBound() const { return arguments_[2]; }

std::string BetweenExpression::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << EncloseArgument(*Value(), mode) << " " << predicate_condition_ << " "
         << EncloseArgument(*LowerBound(), mode) << " AND " << EncloseArgument(*UpperBound(), mode);
  return stream.str();
}

ExpressionPrecedence BetweenExpression::Precedence() const { return ExpressionPrecedence::kBinaryTernaryPredicate; }

std::shared_ptr<AbstractExpression> BetweenExpression::OnDeepCopy() const {
  return std::make_shared<BetweenExpression>(predicate_condition_, Value()->DeepCopy(), LowerBound()->DeepCopy(),
                                             UpperBound()->DeepCopy());
}

}  // namespace skyrise
