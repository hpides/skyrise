/**
 * Taken and modified from our sister project Hyrise (https://github.com/hyrise/hyrise)
 */
#pragma once

#include <cstdint>
#include <limits>
#include <ostream>
#include <vector>

#include "utils/assert.hpp"

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
using ChunkId = uint32_t;

using TaskId = uint32_t;

inline constexpr ColumnId kInvalidColumnId = std::numeric_limits<ColumnId>::max();
inline constexpr TaskId kInvalidTaskId = std::numeric_limits<TaskId>::max();

/**
 * Outputs @param column_ids to @param stream as a comma-separated list.
 * Function is used by multiple Description() implementations.
 */
std::ostream& operator<<(std::ostream& stream, const std::vector<ColumnId>& column_ids);

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

enum class SchedulePriority {
  kDefault = 1,  // Schedule task at the end of the queue.
  kHigh = 0      // Schedule task at the beginning of the queue.
};

std::ostream& operator<<(std::ostream& stream, const PredicateCondition predicate_condition);

/**
 * @return Whether the PredicateCondition takes exactly two arguments.
 */
bool IsBinaryPredicateCondition(const PredicateCondition predicate_condition);

/**
 * @return Whether the PredicateCondition takes exactly two arguments and is not one of LIKE or IN.
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

enum class ExchangeMode { kFullMerge, kPartialMerge, kFullyMeshedExchange };
std::ostream& operator<<(std::ostream& stream, ExchangeMode exchange_mode);

/**
 * Let R and S be two tables and we want to perform R <JoinMode> S ON <condition>
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
 * values, both for ascending and descending sorts.
 */
enum class SortMode { kAscending, kDescending };
std::ostream& operator<<(std::ostream& stream, SortMode sort_mode);

/**
 * Defines in which order a certain column should be or is sorted.
 */
struct SortColumnDefinition final {
  explicit SortColumnDefinition(ColumnId column_id, SortMode sort_mode = SortMode::kAscending)
      : column_id(column_id), sort_mode(sort_mode) {}

  size_t Hash() const;

  ColumnId column_id;
  SortMode sort_mode;
};

inline bool operator==(const SortColumnDefinition& lhs, const SortColumnDefinition& rhs) {
  return lhs.column_id == rhs.column_id && lhs.sort_mode == rhs.sort_mode;
}

/**
 * Defines a general reference to an object stored in S3.
 */
struct ObjectReference {
  explicit ObjectReference(std::string init_bucket_name, std::string init_identifier, std::string init_etag = "")
      : bucket_name(std::move(init_bucket_name)), identifier(std::move(init_identifier)), etag(std::move(init_etag)) {
    Assert(!bucket_name.empty(), "ObjectReference requires a non-empty bucket name.");
    Assert(!identifier.empty(), "ObjectReference requires a non-empty object identifier.");
  }

  bool operator==(const ObjectReference& other) const {
    return bucket_name == other.bucket_name && identifier == other.identifier && etag == other.etag;
  }

  std::string bucket_name;
  std::string identifier;
  std::string etag;
};

}  // namespace skyrise
