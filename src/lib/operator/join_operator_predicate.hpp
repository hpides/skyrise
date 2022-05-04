/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include "types.hpp"

namespace skyrise {

// Predicate representation for Join operators consists of one column of each input side and a join predicate.
// TODO(d-justen): Move JoinOperatorPredicate to hash_join_operator.hpp
struct JoinOperatorPredicate {
  ColumnId column_id_left;
  ColumnId column_id_right;
  PredicateCondition predicate_condition;
};

}  // namespace skyrise
