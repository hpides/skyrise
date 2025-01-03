/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <boost/container_hash/hash.hpp>

#include "types.hpp"

namespace skyrise {

// Predicate representation for Join operators consists of one column of each input side and a join predicate.
// TODO(tobodner): Move JoinOperatorPredicate to hash_join_operator.hpp
struct JoinOperatorPredicate {
  ColumnId column_id_left;
  ColumnId column_id_right;
  PredicateCondition predicate_condition;

  size_t Hash() const {
    size_t hash = boost::hash_value(predicate_condition);
    boost::hash_combine(hash, column_id_left);
    boost::hash_combine(hash, column_id_right);
    return hash;
  }
};

}  // namespace skyrise
