/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "abstract_predicate_expression.hpp"

#include <sstream>

#include <boost/container_hash/hash.hpp>

#include "utils/assert.hpp"

namespace skyrise {

AbstractPredicateExpression::AbstractPredicateExpression(
    const PredicateCondition init_predicate_condition,
    const std::vector<std::shared_ptr<AbstractExpression>>& init_arguments)
    : AbstractExpression(ExpressionType::kPredicate, init_arguments), predicate_condition_(init_predicate_condition) {}

DataType AbstractPredicateExpression::GetDataType() const {
  // TODO(maltenbergert): Revisit DataType once the ExpressionEvaluator is introduced.
  return DataType::kInt;
}

bool AbstractPredicateExpression::ShallowEquals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const AbstractPredicateExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
  return predicate_condition_ == static_cast<const AbstractPredicateExpression&>(expression).predicate_condition_;
}

size_t AbstractPredicateExpression::OnShallowHash() const {
  return boost::hash_value(static_cast<size_t>(predicate_condition_));
}

}  // namespace skyrise
