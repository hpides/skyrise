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

enum class DescriptionMode { kSingleLine, kMultiLine };

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

/**
 * Supported aggregate functions. In addition to the default SQL functions (e.g., MIN(), MAX()), Skyrise internally uses
 * the ANY() function, which expects all values in the group to be equal and returns that value. In SQL terms, this
 * would be an additional, but unnecessary GROUP BY column. This function is only used by the optimizer in case that
 * all values of the group are known to be equal.
 */
enum class AggregateFunction { kAny, kAvg, kCount, kCountDistinct, kMax, kMin, kStandardDeviationSample, kSum };
std::ostream& operator<<(std::ostream& stream, const AggregateFunction aggregate_function);

/**
 * Let R and S be two tables and we want to perform `R <JoinMode> S ON <condition>`
 * kAntiNullAsTrue:    If for a tuple Ri in R, there is a tuple Sj in S so that <condition> is NULL or TRUE, Ri is
 *                      dropped. This behavior mirrors NOT IN.
 * kAntiNullAsFalse:   If for a tuple Ri in R, there is a tuple Sj in S so that <condition> is TRUE, Ri is
 *                      dropped. This behavior mirrors NOT EXISTS
 */
enum class JoinMode { kAntiNullAsFalse, kAntiNullAsTrue, kCross, kFullOuter, kInner, kLeftOuter, kRightOuter, kSemi };
std::ostream& operator<<(std::ostream& stream, const JoinMode join_mode);

/**
 * SQL set operations come in two flavors, with and without `ALL`, e.g., `UNION` and `UNION ALL`.
 */
enum class SetOperationMode { kAll, kUnique };
std::ostream& operator<<(std::ostream& stream, SetOperationMode set_operation_mode);

/**
 * According to the SQL standard, the position of NULLs is implementation-defined. In Skyrise, NULLs come before all
 * values, both for ascending and descending sorts. See sort.cpp for details.
 */
enum class SortMode { kAscending, kDescending };
std::ostream& operator<<(std::ostream& stream, SortMode sort_mode);

/**
 * Defines in which order a certain column should be or is sorted.
 */
struct SortColumnDefinition final {
  explicit SortColumnDefinition(ColumnId column, SortMode sort_mode = SortMode::kAscending)
      : column(column), sort_mode(sort_mode) {}

  ColumnId column;
  SortMode sort_mode;
};

inline bool operator==(const SortColumnDefinition& lhs, const SortColumnDefinition& rhs) {
  return lhs.column == rhs.column && lhs.sort_mode == rhs.sort_mode;
}

}  // namespace skyrise
