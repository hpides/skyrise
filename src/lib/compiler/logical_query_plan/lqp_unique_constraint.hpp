/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "expression/abstract_expression.hpp"
#include "types.hpp"

namespace skyrise {

/**
 * Container structure to define uniqueness for subsets of LQP output expressions. Analogous to SQL's UNIQUE
 * constraint, rows containing NULL values in any of the expressions are always considered to be distinct. For
 * PRIMARY KEY semantics, check if the expressions are nullable, cf. AbstractLqpNode::is_column_nullable.
 *
 * NOTE: Unique constraints are only valid for LQP nodes that contain no invalidated rows (i.e., where there has
 *       been a ValidateNode before or where MVCC is disabled).
 */
struct LqpUniqueConstraint final {
  explicit LqpUniqueConstraint(ExpressionUnorderedSet init_expressions);

  bool operator==(const LqpUniqueConstraint& rhs) const;
  bool operator!=(const LqpUniqueConstraint& rhs) const;
  size_t Hash() const;

  ExpressionUnorderedSet expressions;
};

std::ostream& operator<<(std::ostream& stream, const LqpUniqueConstraint& unique_constraint);

using LqpUniqueConstraints = std::vector<LqpUniqueConstraint>;

}  // namespace skyrise

namespace std {

template <>
struct hash<skyrise::LqpUniqueConstraint> {
  size_t operator()(const skyrise::LqpUniqueConstraint& lqp_unique_constraint) const;
};

}  // namespace std
