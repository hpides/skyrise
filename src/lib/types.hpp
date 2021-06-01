/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <cstdint>
#include <limits>
#include <ostream>

namespace skyrise {

class Noncopyable {
 protected:
  Noncopyable() = default;
  Noncopyable(const Noncopyable&) = delete;
  Noncopyable(Noncopyable&&) noexcept = default;

  Noncopyable& operator=(Noncopyable&&) noexcept = default;
  const Noncopyable& operator=(const Noncopyable&) = delete;

  ~Noncopyable() = default;
};

using ColumnCount = uint32_t;
using ColumnId = uint32_t;

inline constexpr ColumnId kInvalidColumnId = std::numeric_limits<ColumnId>::max();

enum class PredicateCondition {
  kEquals,
  kNotEquals,
  kLessThan,
  kLessThanEquals,
  kGreaterThan,
  kGreaterThanEquals,
  kBetweenInclusive,
  kBetweenLowerExclusive,
  kBetweenUpperExclusive,
  kBetweenExclusive,
  kIn,
  kNotIn,
  kLike,
  kNotLike,
  kIsNull,
  kIsNotNull
};

std::ostream& operator<<(std::ostream& stream, const PredicateCondition predicate_condition);

/**
 * @returns Whether the PredicateCondition takes exactly two arguments.
 */
bool IsBinaryPredicateCondition(const PredicateCondition predicate_condition);

/**
 * @returns Whether the PredicateCondition takes exactly two arguments and is not one of LIKE or IN.
 */
bool IsBinaryNumericPredicateCondition(const PredicateCondition predicate_condition);

bool IsBetweenPredicateCondition(PredicateCondition predicate_condition);

bool IsLowerInclusiveBetween(PredicateCondition predicate_condition);

bool IsUpperInclusiveBetween(PredicateCondition predicate_condition);

/**
 * Flip condition: ">" becomes "<" etc.
 */
PredicateCondition FlipPredicateCondition(const PredicateCondition predicate_condition);

/**
 * Flip condition: ">" becomes "<=" etc.
 */
PredicateCondition InversePredicateCondition(const PredicateCondition predicate_condition);

/**
 * Split up, e.g., BetweenUpperExclusive into {GreaterThanEquals, LessThan}.
 */
std::pair<PredicateCondition, PredicateCondition> BetweenToConditions(const PredicateCondition predicate_condition);

/**
 * Join, e.g., {GreaterThanEquals, LessThan} into BetweenUpperExclusive.
 */
PredicateCondition ConditionsToBetween(const PredicateCondition lower, const PredicateCondition upper);

}  // namespace skyrise
