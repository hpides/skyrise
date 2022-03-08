/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "lqp_unique_constraint.hpp"

#include <boost/container_hash/hash.hpp>

namespace skyrise {

LqpUniqueConstraint::LqpUniqueConstraint(ExpressionUnorderedSet init_expressions)
    : expressions(std::move(init_expressions)) {
  Assert(!expressions.empty(), "LqpUniqueConstraint cannot be empty.");
}

bool LqpUniqueConstraint::operator==(const LqpUniqueConstraint& rhs) const {
  if (expressions.size() != rhs.expressions.size()) return false;
  return std::all_of(expressions.cbegin(), expressions.cend(), [&rhs](const auto column_expression) {
    // TODO(julianmenzler): C++20: Replace with .contains
    return rhs.expressions.find(column_expression) != rhs.expressions.end();
  });
}

bool LqpUniqueConstraint::operator!=(const LqpUniqueConstraint& rhs) const { return !(rhs == *this); }

size_t LqpUniqueConstraint::hash() const {
  size_t hash = 0;
  for (const auto& expression : expressions) {
    // To make the hash independent of the expressions' order, we have to use a commutative operator like XOR.
    hash = hash ^ expression->Hash();
  }

  return boost::hash_value(hash - expressions.size());
}

std::ostream& operator<<(std::ostream& stream, const LqpUniqueConstraint& unique_constraint) {
  stream << "{";
  auto expressions_vector = std::vector<std::shared_ptr<AbstractExpression>>{unique_constraint.expressions.begin(),
                                                                             unique_constraint.expressions.end()};
  stream << expressions_vector.at(0)->AsColumnName();
  for (auto expression_idx = size_t{1}; expression_idx < expressions_vector.size(); ++expression_idx) {
    stream << ", " << expressions_vector[expression_idx]->AsColumnName();
  }
  stream << "}";

  return stream;
}

}  // namespace skyrise

namespace std {

size_t hash<skyrise::LqpUniqueConstraint>::operator()(const skyrise::LqpUniqueConstraint& lqp_unique_constraint) const {
  return lqp_unique_constraint.hash();
}

}  // namespace std
