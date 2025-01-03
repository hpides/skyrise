/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#include "types.hpp"

#include <boost/container_hash/hash.hpp>

#include "constant_mappings.hpp"
#include "utils/assert.hpp"

namespace skyrise {

std::ostream& operator<<(std::ostream& stream, const std::vector<ColumnId>& column_ids) {
  for (size_t i = 0; i < column_ids.size(); ++i) {
    if (column_ids[i] == kInvalidColumnId) {
      stream << "kInvalidColumnId";
    } else {
      stream << column_ids[i];
    }
    if (i + 1 < column_ids.size()) {
      stream << ", ";
    }
  }
  return stream;
}

bool IsBinaryPredicateCondition(const PredicateCondition predicate_condition) {
  return predicate_condition == PredicateCondition::kEquals || predicate_condition == PredicateCondition::kNotEquals ||
         predicate_condition == PredicateCondition::kLessThan ||
         predicate_condition == PredicateCondition::kLessThanEquals ||
         predicate_condition == PredicateCondition::kGreaterThan ||
         predicate_condition == PredicateCondition::kGreaterThanEquals ||
         predicate_condition == PredicateCondition::kNotLike || predicate_condition == PredicateCondition::kLike ||
         predicate_condition == PredicateCondition::kIn || predicate_condition == PredicateCondition::kNotIn;
}

bool IsBinaryNumericPredicateCondition(const PredicateCondition predicate_condition) {
  return predicate_condition == PredicateCondition::kEquals || predicate_condition == PredicateCondition::kNotEquals ||
         predicate_condition == PredicateCondition::kLessThan ||
         predicate_condition == PredicateCondition::kLessThanEquals ||
         predicate_condition == PredicateCondition::kGreaterThan ||
         predicate_condition == PredicateCondition::kGreaterThanEquals;
}

bool IsBetweenPredicateCondition(PredicateCondition predicate_condition) {
  return predicate_condition == PredicateCondition::kBetweenInclusive ||
         predicate_condition == PredicateCondition::kBetweenLowerExclusive ||
         predicate_condition == PredicateCondition::kBetweenUpperExclusive ||
         predicate_condition == PredicateCondition::kBetweenExclusive;
}

bool IsLowerInclusiveBetween(PredicateCondition predicate_condition) {
  return predicate_condition == PredicateCondition::kBetweenInclusive ||
         predicate_condition == PredicateCondition::kBetweenUpperExclusive;
}

bool IsUpperInclusiveBetween(PredicateCondition predicate_condition) {
  return predicate_condition == PredicateCondition::kBetweenInclusive ||
         predicate_condition == PredicateCondition::kBetweenLowerExclusive;
}

PredicateCondition FlipPredicateCondition(const PredicateCondition predicate_condition) {
  switch (predicate_condition) {
    case PredicateCondition::kEquals:
      return PredicateCondition::kEquals;
    case PredicateCondition::kNotEquals:
      return PredicateCondition::kNotEquals;
    case PredicateCondition::kLessThan:
      return PredicateCondition::kGreaterThan;
    case PredicateCondition::kLessThanEquals:
      return PredicateCondition::kGreaterThanEquals;
    case PredicateCondition::kGreaterThan:
      return PredicateCondition::kLessThan;
    case PredicateCondition::kGreaterThanEquals:
      return PredicateCondition::kLessThanEquals;

    case PredicateCondition::kBetweenInclusive:
    case PredicateCondition::kBetweenLowerExclusive:
    case PredicateCondition::kBetweenUpperExclusive:
    case PredicateCondition::kBetweenExclusive:
    case PredicateCondition::kIn:
    case PredicateCondition::kNotIn:
    case PredicateCondition::kLike:
    case PredicateCondition::kNotLike:
    case PredicateCondition::kIsNull:
    case PredicateCondition::kIsNotNull:
      Fail("Can't flip specified PredicateCondition");
  }
  Fail("Invalid enum value");
}

PredicateCondition InversePredicateCondition(const PredicateCondition predicate_condition) {
  switch (predicate_condition) {
    case PredicateCondition::kEquals:
      return PredicateCondition::kNotEquals;
    case PredicateCondition::kNotEquals:
      return PredicateCondition::kEquals;
    case PredicateCondition::kGreaterThan:
      return PredicateCondition::kLessThanEquals;
    case PredicateCondition::kLessThanEquals:
      return PredicateCondition::kGreaterThan;
    case PredicateCondition::kGreaterThanEquals:
      return PredicateCondition::kLessThan;
    case PredicateCondition::kLessThan:
      return PredicateCondition::kGreaterThanEquals;
    case PredicateCondition::kLike:
      return PredicateCondition::kNotLike;
    case PredicateCondition::kNotLike:
      return PredicateCondition::kLike;
    case PredicateCondition::kIsNull:
      return PredicateCondition::kIsNotNull;
    case PredicateCondition::kIsNotNull:
      return PredicateCondition::kIsNull;
    case PredicateCondition::kIn:
      return PredicateCondition::kNotIn;
    case PredicateCondition::kNotIn:
      return PredicateCondition::kIn;

    default:
      Fail("Can't inverse the specified PredicateCondition");
  }
}

std::pair<PredicateCondition, PredicateCondition> BetweenToConditions(const PredicateCondition predicate_condition) {
  switch (predicate_condition) {
    case PredicateCondition::kBetweenInclusive:
      return {PredicateCondition::kGreaterThanEquals, PredicateCondition::kLessThanEquals};
    case PredicateCondition::kBetweenLowerExclusive:
      return {PredicateCondition::kGreaterThan, PredicateCondition::kLessThanEquals};
    case PredicateCondition::kBetweenUpperExclusive:
      return {PredicateCondition::kGreaterThanEquals, PredicateCondition::kLessThan};
    case PredicateCondition::kBetweenExclusive:
      return {PredicateCondition::kGreaterThan, PredicateCondition::kLessThan};
    default:
      Fail("Input was not a between condition");
  }
}

PredicateCondition ConditionsToBetween(const PredicateCondition lower, const PredicateCondition upper) {
  if (lower == PredicateCondition::kGreaterThan) {
    if (upper == PredicateCondition::kLessThan) {
      return PredicateCondition::kBetweenExclusive;
    } else if (upper == PredicateCondition::kLessThanEquals) {
      return PredicateCondition::kBetweenLowerExclusive;
    }
  } else if (lower == PredicateCondition::kGreaterThanEquals) {
    if (upper == PredicateCondition::kLessThan) {
      return PredicateCondition::kBetweenUpperExclusive;
    } else if (upper == PredicateCondition::kLessThanEquals) {
      return PredicateCondition::kBetweenInclusive;
    }
  }
  Fail("Unexpected PredicateCondition");
}

std::ostream& operator<<(std::ostream& stream, const PredicateCondition predicate_condition) {
  return stream << kPredicateConditionToString.left.at(predicate_condition);
}

std::ostream& operator<<(std::ostream& stream, const AggregateFunction aggregate_function) {
  return stream << kAggregateFunctionToString.left.at(aggregate_function);
}

std::ostream& operator<<(std::ostream& stream, const JoinMode join_mode) {
  return stream << kJoinModeToString.left.at(join_mode);
}

std::ostream& operator<<(std::ostream& stream, ExchangeMode exchange_mode) {
  return stream << kExchangeModeToString.left.at(exchange_mode);
}

std::ostream& operator<<(std::ostream& stream, SetOperationMode set_operation_mode) {
  return stream << kSetOperationModeToString.left.at(set_operation_mode);
}

size_t SortColumnDefinition::Hash() const {
  size_t hash = boost::hash_value(column_id);
  boost::hash_combine(hash, sort_mode);
  return hash;
}

std::ostream& operator<<(std::ostream& stream, SortMode sort_mode) {
  return stream << kSortModeToString.left.at(sort_mode);
}

}  // namespace skyrise
