/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "between_expression.hpp"

#include <sstream>

#include "types.hpp"
#include "utils/assert.hpp"

namespace skyrise {

BetweenExpression::BetweenExpression(const PredicateCondition predicate_condition,
                                     std::shared_ptr<AbstractExpression> value,
                                     std::shared_ptr<AbstractExpression> lower_bound,
                                     std::shared_ptr<AbstractExpression> upper_bound)
    : AbstractPredicateExpression(predicate_condition,
                                  {std::move(value), std::move(lower_bound), std::move(upper_bound)}) {
  Assert(IsBetweenPredicateCondition(predicate_condition_), "Unsupported PredicateCondition.");
}

const std::shared_ptr<AbstractExpression>& BetweenExpression::Value() const { return arguments_[0]; }

const std::shared_ptr<AbstractExpression>& BetweenExpression::LowerBound() const { return arguments_[1]; }

const std::shared_ptr<AbstractExpression>& BetweenExpression::UpperBound() const { return arguments_[2]; }

std::shared_ptr<AbstractExpression> BetweenExpression::DeepCopy() const {
  return std::make_shared<BetweenExpression>(predicate_condition_, Value()->DeepCopy(), LowerBound()->DeepCopy(),
                                             UpperBound()->DeepCopy());
}

std::string BetweenExpression::Description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << EncloseArgument(*Value(), mode) << " " << predicate_condition_ << " "
         << EncloseArgument(*LowerBound(), mode) << " AND " << EncloseArgument(*UpperBound(), mode);
  return stream.str();
}

ExpressionPrecedence BetweenExpression::Precedence() const { return ExpressionPrecedence::kBinaryTernaryPredicate; }

}  // namespace skyrise
