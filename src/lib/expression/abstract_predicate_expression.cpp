/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "abstract_predicate_expression.hpp"

#include <sstream>

#include <boost/container_hash/hash.hpp>

#include "utils/assert.hpp"

namespace skyrise {

AbstractPredicateExpression::AbstractPredicateExpression(const PredicateCondition predicate_condition,
                                                         std::vector<std::shared_ptr<AbstractExpression>> arguments)
    : AbstractExpression(ExpressionType::kPredicate, std::move(arguments)), predicate_condition_(predicate_condition) {}

DataType AbstractPredicateExpression::GetDataType() const { return DataType::kInt; }

PredicateCondition AbstractPredicateExpression::GetPredicateCondition() const { return predicate_condition_; }

bool AbstractPredicateExpression::ShallowEquals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const AbstractPredicateExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==.");
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
  return predicate_condition_ == static_cast<const AbstractPredicateExpression&>(expression).predicate_condition_;
}

size_t AbstractPredicateExpression::ShallowHash() const {
  return boost::hash_value(static_cast<size_t>(predicate_condition_));
}

}  // namespace skyrise
